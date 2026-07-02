//! DynamoDB Streams payload for `core::Record.data`.
//!
//! The typed model ([`AttrValue`], [`Item`], [`StreamRecord`]) now lives in
//! `ddbstreams-kcl-core` so the DDB source and the binding/wire layer share one
//! type. This module re-exports it and adds the `aws-sdk-dynamodbstreams` →
//! [`StreamRecord`] converter behind the `aws` feature (it needs the SDK types).

pub use ddbstreams_kcl_core::record::{AttrValue, Item, StreamRecord};

#[cfg(feature = "aws")]
pub use from_sdk::from_sdk;

#[cfg(feature = "aws")]
mod from_sdk {
    use super::{AttrValue, Item, StreamRecord};
    use aws_sdk_dynamodbstreams::types::{AttributeValue as Sdk, Record as SdkRecord};

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
            AttrValue::M(map_btree(m))
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

    fn map_btree(m: &std::collections::HashMap<String, Sdk>) -> Item {
        m.iter().map(|(k, v)| (k.clone(), attr(v))).collect()
    }

    /// Convert an `aws-sdk-dynamodbstreams` record into the typed core model.
    pub fn from_sdk(r: &SdkRecord) -> StreamRecord {
        let event_name = r.event_name().map(|e| e.as_str().to_string());
        let sr = r.dynamodb();
        StreamRecord {
            event_name,
            sequence_number: sr.and_then(|d| d.sequence_number()).map(|s| s.to_string()),
            size_bytes: sr.and_then(|d| d.size_bytes()),
            stream_view_type: sr.and_then(|d| d.stream_view_type()).map(|v| v.as_str().to_string()),
            keys: sr.and_then(|d| d.keys()).map(map_btree).unwrap_or_default(),
            new_image: sr.and_then(|d| d.new_image()).filter(|m| !m.is_empty()).map(map_btree),
            old_image: sr.and_then(|d| d.old_image()).filter(|m| !m.is_empty()).map(map_btree),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::super::{AttrValue, StreamRecord};
        use aws_sdk_dynamodbstreams::primitives::Blob;
        use aws_sdk_dynamodbstreams::types::{
            AttributeValue as Sdk, OperationType, Record as SdkRecord, StreamRecord as SdkStreamRecord,
            StreamViewType,
        };
        use std::collections::HashMap;

        // Every DynamoDB attribute type maps to the right AttrValue variant.
        #[test]
        fn from_sdk_maps_all_attribute_types_and_images() {
            let mut m = HashMap::new();
            m.insert("inner".to_string(), Sdk::N("7".into()));

            let mut keys = HashMap::new();
            keys.insert("s".to_string(), Sdk::S("str".into()));
            keys.insert("n".to_string(), Sdk::N("42".into()));
            keys.insert("bool".to_string(), Sdk::Bool(true));
            keys.insert("null".to_string(), Sdk::Null(true));
            keys.insert("b".to_string(), Sdk::B(Blob::new(vec![1u8, 2, 3])));
            keys.insert("m".to_string(), Sdk::M(m));
            keys.insert("l".to_string(), Sdk::L(vec![Sdk::S("a".into()), Sdk::Null(true)]));
            keys.insert("ss".to_string(), Sdk::Ss(vec!["x".into(), "y".into()]));
            keys.insert("ns".to_string(), Sdk::Ns(vec!["1".into(), "2".into()]));
            keys.insert("bs".to_string(), Sdk::Bs(vec![Blob::new(vec![9u8])]));

            let mut new_image = HashMap::new();
            new_image.insert("s".to_string(), Sdk::S("str".into()));

            let sr = SdkStreamRecord::builder()
                .sequence_number("100")
                .size_bytes(64)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .set_keys(Some(keys))
                .set_new_image(Some(new_image))
                .build();
            let rec = SdkRecord::builder()
                .event_name(OperationType::Insert)
                .dynamodb(sr)
                .build();

            let out: StreamRecord = super::from_sdk(&rec);

            assert_eq!(out.event_name.as_deref(), Some("INSERT"));
            assert_eq!(out.sequence_number.as_deref(), Some("100"));
            assert_eq!(out.size_bytes, Some(64));
            assert_eq!(out.stream_view_type.as_deref(), Some("NEW_AND_OLD_IMAGES"));
            assert_eq!(out.keys.get("s"), Some(&AttrValue::S("str".into())));
            assert_eq!(out.keys.get("n"), Some(&AttrValue::N("42".into())));
            assert_eq!(out.keys.get("bool"), Some(&AttrValue::Bool(true)));
            assert_eq!(out.keys.get("null"), Some(&AttrValue::Null));
            assert_eq!(out.keys.get("b"), Some(&AttrValue::B(vec![1, 2, 3])));
            assert_eq!(out.keys.get("ss"), Some(&AttrValue::Ss(vec!["x".into(), "y".into()])));
            assert_eq!(out.keys.get("ns"), Some(&AttrValue::Ns(vec!["1".into(), "2".into()])));
            assert_eq!(out.keys.get("bs"), Some(&AttrValue::Bs(vec![vec![9]])));
            match out.keys.get("m") {
                Some(AttrValue::M(inner)) => assert_eq!(inner.get("inner"), Some(&AttrValue::N("7".into()))),
                other => panic!("expected M, got {other:?}"),
            }
            match out.keys.get("l") {
                Some(AttrValue::L(items)) => {
                    assert_eq!(items[0], AttrValue::S("a".into()));
                    assert_eq!(items[1], AttrValue::Null);
                }
                other => panic!("expected L, got {other:?}"),
            }
            // new_image present, old_image absent (empty → None).
            assert!(out.new_image.is_some());
            assert!(out.old_image.is_none());
        }

        // Empty new/old images are normalized to None (not an empty map).
        #[test]
        fn from_sdk_empty_images_are_none() {
            let sr = SdkStreamRecord::builder().sequence_number("1").build();
            let rec = SdkRecord::builder().dynamodb(sr).build();
            let out = super::from_sdk(&rec);
            assert!(out.new_image.is_none());
            assert!(out.old_image.is_none());
            assert!(out.keys.is_empty());
        }
    }
}
