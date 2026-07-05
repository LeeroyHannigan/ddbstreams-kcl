using System.Text.Json;
using Amazon.DynamoDBv2.Model;
using Xunit;

namespace Amazon.DynamoDBStreams.Consumer.Tests;

public class SdkAttributeValuesTests
{
    private const string WireItem = """
    {
      "s": {"S": "widget"},
      "n": {"N": "42"},
      "active": {"Bool": true},
      "deleted": "Null",
      "blob": {"B": [1, 2, 3]},
      "tags": {"Ss": ["a", "b"]},
      "meta": {"M": {"k": {"S": "v"}}},
      "list": {"L": [{"N": "7"}, "Null"]}
    }
    """;

    [Fact]
    public void DecodesToSdkAttributeValues()
    {
        var wire = JsonDocument.Parse(WireItem).RootElement;
        Dictionary<string, AttributeValue> item =
            SdkAttributeValues.ToItem(SdkAttributeValues.DecodeItem(wire));

        Assert.Equal("widget", item["s"].S);
        Assert.Equal("42", item["n"].N);
        Assert.True(item["active"].BOOL);
        Assert.True(item["deleted"].NULL);
        Assert.Equal(new byte[] { 1, 2, 3 }, item["blob"].B.ToArray());
        Assert.Equal(new List<string> { "a", "b" }, item["tags"].SS);
        Assert.Equal("v", item["meta"].M["k"].S);
        Assert.Equal("7", item["list"].L[0].N);
        Assert.True(item["list"].L[1].NULL);
    }
}
