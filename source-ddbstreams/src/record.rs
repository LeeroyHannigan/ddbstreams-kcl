//! Typed DynamoDB Streams change record carried in `core::Record.data`.
//!
//! Follows KCL's `RecordAdapter` pattern: the engine treats the payload as
//! opaque bytes, and this DDB layer encodes/decodes the real `StreamRecord`.
//! [`AttrValue`] mirrors the DynamoDB attribute model exactly (S/N/B/BOOL/NULL/
//! M/L/SS/NS/BS), so item images round-trip losslessly.
//!
//! The pure model + encode/decode are always built and unit-tested offline; the
//! `from_sdk` converter (from `aws-sdk-dynamodbstreams`) is behind `aws`.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A DynamoDB attribute value (the full type set). `BTreeMap` keeps map key
/// order deterministic for stable encoding/tests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    S(String),
    /// Numbers are carried as their canonical string form (as DynamoDB does).
    N(String),
    Bool(bool),
    Null,
    B(Vec<u8>),
    M(BTreeMap<String, AttrValue>),
    L(Vec<AttrValue>),
    Ss(Vec<String>),
    Ns(Vec<String>),
    Bs(Vec<Vec<u8>>),
}

pub type Item = BTreeMap<String, AttrValue>;

/// One item-level change from a DynamoDB stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct StreamRecord {
    /// INSERT / MODIFY / REMOVE.
    pub event_name: Option<String>,
    pub sequence_number: Option<String>,
    pub size_bytes: Option<i64>,
    /// KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES.
    pub stream_view_type: Option<String>,
    pub keys: Item,
    pub new_image: Option<Item>,
    pub old_image: Option<Item>,
}

impl StreamRecord {
    /// Encode into the opaque bytes carried by `core::Record.data`.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("StreamRecord serialize")
    }

    /// Decode a `core::Record.data` payload produced by [`StreamRecord::encode`].
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(feature = "aws")]
mod from_sdk {
    use super::{AttrValue, Item, StreamRecord};
    use aws_sdk_dynamodbstreams::types::{AttributeValue as Sdk, Record as SdkRecord};
    use std::collections::BTreeMap;

    fn attr(av: &Sdk) -> AttrValue {
        if let Ok(s) = av.as_s() {
            AttrValue::S(s.clone())
        } else if let Ok(n) = av.as_n() {
            AttrValue::N(n.clone())
        } else if let Ok(b) = av.as_bool() {
            AttrValue::Bool(*b)
        } else if av.as_null().is_ok() {
            AttrValue::Null
        } else if let Ok(b) = av.as_b() {
            AttrValue::B(b.as_ref().to_vec())
        } else if let Ok(m) = av.as_m() {
            AttrValue::M(map(m))
        } else if let Ok(l) = av.as_l() {
            AttrValue::L(l.iter().map(attr).collect())
        } else if let Ok(ss) = av.as_ss() {
            AttrValue::Ss(ss.clone())
        } else if let Ok(ns) = av.as_ns() {
            AttrValue::Ns(ns.clone())
        } else if let Ok(bs) = av.as_bs() {
            AttrValue::Bs(bs.iter().map(|b| b.as_ref().to_vec()).collect())
        } else {
            AttrValue::Null
        }
    }

    fn map(m: &std::collections::HashMap<String, Sdk>) -> Item {
        m.iter().map(|(k, v)| (k.clone(), attr(v))).collect()
    }

    impl StreamRecord {
        /// Convert an `aws-sdk-dynamodbstreams` record into the typed model.
        pub fn from_sdk(r: &SdkRecord) -> Self {
            let event_name = r.event_name().map(|e| e.as_str().to_string());
            let sr = r.dynamodb();
            StreamRecord {
                event_name,
                sequence_number: sr.and_then(|d| d.sequence_number()).map(|s| s.to_string()),
                size_bytes: sr.and_then(|d| d.size_bytes()),
                stream_view_type: sr
                    .and_then(|d| d.stream_view_type())
                    .map(|v| v.as_str().to_string()),
                keys: sr
                    .and_then(|d| d.keys())
                    .map(map)
                    .unwrap_or_default(),
                new_image: sr
                    .and_then(|d| d.new_image())
                    .filter(|m| !m.is_empty())
                    .map(map),
                old_image: sr
                    .and_then(|d| d.old_image())
                    .filter(|m| !m.is_empty())
                    .map(map),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_nested_item_images() {
        let mut keys = Item::new();
        keys.insert("pk".into(), AttrValue::S("k1".into()));
        keys.insert("sk".into(), AttrValue::N("42".into()));

        let mut new_image = keys.clone();
        new_image.insert("active".into(), AttrValue::Bool(true));
        new_image.insert("tags".into(), AttrValue::Ss(vec!["a".into(), "b".into()]));
        new_image.insert(
            "nested".into(),
            AttrValue::M(BTreeMap::from([
                ("count".to_string(), AttrValue::N("3".into())),
                ("list".to_string(), AttrValue::L(vec![AttrValue::Null, AttrValue::S("x".into())])),
            ])),
        );

        let rec = StreamRecord {
            event_name: Some("MODIFY".into()),
            sequence_number: Some("100000000000000000001".into()),
            size_bytes: Some(128),
            stream_view_type: Some("NEW_AND_OLD_IMAGES".into()),
            keys,
            new_image: Some(new_image),
            old_image: None,
        };

        let decoded = StreamRecord::decode(&rec.encode()).unwrap();
        assert_eq!(decoded, rec);
    }

    #[test]
    fn binary_and_number_set_round_trip() {
        let mut keys = Item::new();
        keys.insert("b".into(), AttrValue::B(vec![0, 1, 2, 255]));
        keys.insert("ns".into(), AttrValue::Ns(vec!["1".into(), "2.5".into()]));
        let rec = StreamRecord { keys, ..Default::default() };
        assert_eq!(StreamRecord::decode(&rec.encode()).unwrap(), rec);
    }
}
