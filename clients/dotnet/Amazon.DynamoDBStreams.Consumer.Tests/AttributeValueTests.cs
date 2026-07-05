using System.Text.Json;
using Xunit;

namespace Amazon.DynamoDBStreams.Consumer.Tests;

public class AttributeValueTests
{
    // A wire item exercising every AttrValue variant, in serde-externally-tagged
    // form (matches protocol/src/lib.rs and conformance fixtures).
    private const string WireItem = """
    {
      "s": {"S": "widget"},
      "n": {"N": "42"},
      "active": {"Bool": true},
      "deleted": "Null",
      "blob": {"B": [1, 2, 3]},
      "tags": {"Ss": ["a", "b"]},
      "scores": {"Ns": ["1", "2.5"]},
      "blobs": {"Bs": [[1, 2], [3]]},
      "meta": {"M": {"k": {"S": "v"}}},
      "list": {"L": [{"N": "7"}, "Null"]}
    }
    """;

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    [Fact]
    public void Native_HasNoTypeWrappers_AndKeepsNumbersAsStrings()
    {
        var item = AttributeValueConverter.DecodeItem(Parse(WireItem), RecordFormat.Native);

        Assert.Equal("widget", item["s"]);
        Assert.Equal("42", item["n"]); // number stays a string (lossless)
        Assert.Equal(true, item["active"]);
        Assert.Null(item["deleted"]);
        Assert.Equal(new byte[] { 1, 2, 3 }, Assert.IsType<byte[]>(item["blob"]));
        Assert.Equal(new List<string?> { "a", "b" }, item["tags"]);
        Assert.Equal(new List<string?> { "1", "2.5" }, item["scores"]);

        var blobs = Assert.IsType<List<byte[]>>(item["blobs"]);
        Assert.Equal(new byte[] { 1, 2 }, blobs[0]);
        Assert.Equal(new byte[] { 3 }, blobs[1]);

        var meta = Assert.IsType<Dictionary<string, object?>>(item["meta"]);
        Assert.Equal("v", meta["k"]);

        var list = Assert.IsType<List<object?>>(item["list"]);
        Assert.Equal("7", list[0]);
        Assert.Null(list[1]);
    }

    [Fact]
    public void DdbJson_IsCanonicalTypedForm()
    {
        var item = AttributeValueConverter.DecodeItem(Parse(WireItem), RecordFormat.DdbJson);

        Assert.Equal("widget", Wrapper(item["s"], "S"));
        Assert.Equal("42", Wrapper(item["n"], "N"));
        Assert.Equal(true, Wrapper(item["active"], "BOOL"));
        Assert.Equal(true, Wrapper(item["deleted"], "NULL"));
        Assert.Equal("AQID", Wrapper(item["blob"], "B")); // base64 of [1,2,3]

        var ss = Assert.IsType<List<string?>>(Wrapper(item["tags"], "SS"));
        Assert.Equal(new List<string?> { "a", "b" }, ss);

        var bs = Assert.IsType<List<string>>(Wrapper(item["blobs"], "BS"));
        Assert.Equal(new List<string> { "AQI=", "Aw==" }, bs);

        var m = Assert.IsType<Dictionary<string, object?>>(Wrapper(item["meta"], "M"));
        Assert.Equal("v", Wrapper(m["k"], "S"));

        var l = Assert.IsType<List<object?>>(Wrapper(item["list"], "L"));
        Assert.Equal("7", Wrapper(l[0], "N"));
        Assert.Equal(true, Wrapper(l[1], "NULL"));
    }

    // Unwrap a single-tag DynamoDB-JSON dictionary, asserting the tag matches.
    private static object? Wrapper(object? v, string tag)
    {
        var dict = Assert.IsType<Dictionary<string, object?>>(v);
        Assert.True(dict.ContainsKey(tag), $"expected tag {tag}, got [{string.Join(",", dict.Keys)}]");
        return dict[tag];
    }
}
