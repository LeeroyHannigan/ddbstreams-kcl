using System.Text.Json;

namespace Amazon.DynamoDBStreams.Consumer;

/// <summary>
/// Converts serde-externally-tagged wire attribute values (see
/// <c>protocol/src/lib.rs</c>) into either native .NET objects or canonical
/// DynamoDB JSON. The bare string <c>"Null"</c> is the null variant; every other
/// variant is a single-key object like <c>{"S":"x"}</c>. Byte values arrive as
/// JSON arrays of integers.
/// </summary>
internal static class AttributeValueConverter
{
    public static Dictionary<string, object?> DecodeItem(JsonElement item, RecordFormat fmt)
    {
        var map = new Dictionary<string, object?>();
        if (item.ValueKind != JsonValueKind.Object)
        {
            return map;
        }
        foreach (var prop in item.EnumerateObject())
        {
            map[prop.Name] = fmt == RecordFormat.DdbJson ? ToDdbJson(prop.Value) : ToNative(prop.Value);
        }
        return map;
    }

    private static (string Tag, JsonElement Value) SingleTag(JsonElement v)
    {
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
        return (tag, val);
    }

    private static object? ToNative(JsonElement v)
    {
        if (v.ValueKind == JsonValueKind.String)
        {
            if (v.GetString() == "Null")
            {
                return null;
            }
            throw new FormatException($"invalid attribute value: {v}");
        }
        var (tag, val) = SingleTag(v);
        return tag switch
        {
            "S" or "N" => val.GetString(),
            "Bool" => val.GetBoolean(),
            "B" => Bytes(val),
            "Ss" or "Ns" => StringList(val),
            "Bs" => ByteArrayList(val),
            "M" => DecodeItem(val, RecordFormat.Native),
            "L" => NativeList(val),
            _ => throw new FormatException($"unknown attribute type tag: {tag}"),
        };
    }

    private static object ToDdbJson(JsonElement v)
    {
        if (v.ValueKind == JsonValueKind.String)
        {
            if (v.GetString() == "Null")
            {
                return new Dictionary<string, object?> { ["NULL"] = true };
            }
            throw new FormatException($"invalid attribute value: {v}");
        }
        var (tag, val) = SingleTag(v);
        return tag switch
        {
            "S" => new Dictionary<string, object?> { ["S"] = val.GetString() },
            "N" => new Dictionary<string, object?> { ["N"] = val.GetString() },
            "Bool" => new Dictionary<string, object?> { ["BOOL"] = val.GetBoolean() },
            "B" => new Dictionary<string, object?> { ["B"] = Base64(val) },
            "Ss" => new Dictionary<string, object?> { ["SS"] = StringList(val) },
            "Ns" => new Dictionary<string, object?> { ["NS"] = StringList(val) },
            "Bs" => new Dictionary<string, object?> { ["BS"] = Base64List(val) },
            "M" => new Dictionary<string, object?> { ["M"] = DecodeItem(val, RecordFormat.DdbJson) },
            "L" => new Dictionary<string, object?> { ["L"] = DdbJsonList(val) },
            _ => throw new FormatException($"unknown attribute type tag: {tag}"),
        };
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

    private static List<string?> StringList(JsonElement arr)
    {
        var list = new List<string?>();
        foreach (var e in arr.EnumerateArray())
        {
            list.Add(e.GetString());
        }
        return list;
    }

    private static List<byte[]> ByteArrayList(JsonElement arr)
    {
        var list = new List<byte[]>();
        foreach (var e in arr.EnumerateArray())
        {
            list.Add(Bytes(e));
        }
        return list;
    }

    private static List<object?> NativeList(JsonElement arr)
    {
        var list = new List<object?>();
        foreach (var e in arr.EnumerateArray())
        {
            list.Add(ToNative(e));
        }
        return list;
    }

    private static List<object?> DdbJsonList(JsonElement arr)
    {
        var list = new List<object?>();
        foreach (var e in arr.EnumerateArray())
        {
            list.Add(ToDdbJson(e));
        }
        return list;
    }

    private static string Base64(JsonElement arr) => Convert.ToBase64String(Bytes(arr));

    private static List<string> Base64List(JsonElement arr)
    {
        var list = new List<string>();
        foreach (var e in arr.EnumerateArray())
        {
            list.Add(Base64(e));
        }
        return list;
    }
}
