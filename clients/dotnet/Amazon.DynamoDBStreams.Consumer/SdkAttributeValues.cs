using System.Text.Json;
using Amazon.DynamoDBv2.Model;

namespace Amazon.DynamoDBStreams.Consumer;

/// <summary>
/// Converts serde-externally-tagged wire attribute values into the AWS SDK for
/// .NET typed model (<see cref="AttributeValue"/>) for <see cref="RecordFormat.Sdk"/>.
/// A record decoded in SDK mode carries <see cref="AttributeValue"/> instances as
/// its image values, so they drop straight into the SDK.
/// </summary>
public static class SdkAttributeValues
{
    /// <summary>
    /// View a record image (decoded in <see cref="RecordFormat.Sdk"/>) as a typed
    /// <c>Dictionary&lt;string, AttributeValue&gt;</c> ready for the SDK, e.g.
    /// <c>ddb.PutItemAsync("Orders", SdkAttributeValues.ToItem(r.NewImage))</c>.
    /// </summary>
    /// <exception cref="InvalidCastException">if the image was not decoded in Sdk mode.</exception>
    public static Dictionary<string, AttributeValue> ToItem(IReadOnlyDictionary<string, object?>? image)
    {
        var map = new Dictionary<string, AttributeValue>();
        if (image is null)
        {
            return map;
        }
        foreach (var kv in image)
        {
            map[kv.Key] = (AttributeValue)kv.Value!;
        }
        return map;
    }

    internal static Dictionary<string, object?> DecodeItem(JsonElement item)
    {
        var map = new Dictionary<string, object?>();
        if (item.ValueKind != JsonValueKind.Object)
        {
            return map;
        }
        foreach (var prop in item.EnumerateObject())
        {
            map[prop.Name] = ToAttributeValue(prop.Value);
        }
        return map;
    }

    private static AttributeValue ToAttributeValue(JsonElement v)
    {
        if (v.ValueKind == JsonValueKind.String)
        {
            if (v.GetString() == "Null")
            {
                return new AttributeValue { NULL = true };
            }
            throw new FormatException($"invalid attribute value: {v}");
        }
        if (v.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException($"invalid attribute value: {v}");
        }

        string? tag = null;
        JsonElement val = default;
        var count = 0;
        foreach (var p in v.EnumerateObject())
        {
            tag = p.Name;
            val = p.Value;
            count++;
        }
        if (count != 1 || tag is null)
        {
            throw new FormatException($"attribute must have exactly one type tag, got {count}");
        }

        switch (tag)
        {
            case "S":
                return new AttributeValue { S = val.GetString() };
            case "N":
                return new AttributeValue { N = val.GetString() };
            case "Bool":
                return new AttributeValue { BOOL = val.GetBoolean() };
            case "B":
                return new AttributeValue { B = new MemoryStream(Bytes(val)) };
            case "Ss":
                return new AttributeValue { SS = StringList(val) };
            case "Ns":
                return new AttributeValue { NS = StringList(val) };
            case "Bs":
                return new AttributeValue { BS = val.EnumerateArray().Select(e => new MemoryStream(Bytes(e))).ToList() };
            case "M":
            {
                var m = new Dictionary<string, AttributeValue>();
                foreach (var p in val.EnumerateObject())
                {
                    m[p.Name] = ToAttributeValue(p.Value);
                }
                return new AttributeValue { M = m };
            }
            case "L":
                return new AttributeValue { L = val.EnumerateArray().Select(ToAttributeValue).ToList() };
            default:
                throw new FormatException($"unknown attribute type tag: {tag}");
        }
    }

    private static byte[] Bytes(JsonElement arr)
    {
        var buf = new List<byte>();
        foreach (var e in arr.EnumerateArray())
        {
            buf.Add((byte)e.GetInt32());
        }
        return buf.ToArray();
    }

    private static List<string> StringList(JsonElement arr)
    {
        var list = new List<string>();
        foreach (var e in arr.EnumerateArray())
        {
            list.Add(e.GetString()!);
        }
        return list;
    }
}
