namespace Amazon.DynamoDBStreams.Consumer;

/// <summary>
/// How item-image attribute values are surfaced on a <see cref="Record"/>. Set
/// once at the <see cref="WorkerConfig"/> level; applies to every record.
/// Mirrors the <c>record_format</c> option in the Python/Go/Node clients.
/// </summary>
public enum RecordFormat
{
    /// <summary>
    /// Plain .NET values: <c>S</c>/<c>N</c> → <see cref="string"/> (numbers stay
    /// canonical strings, lossless), <c>Bool</c> → <see cref="bool"/>, <c>Null</c>
    /// → <c>null</c>, <c>B</c> → <see cref="byte"/>[], sets → lists, <c>M</c> →
    /// dictionary, <c>L</c> → list. No <c>{"S": ...}</c> type wrappers.
    /// </summary>
    Native,

    /// <summary>
    /// Canonical DynamoDB JSON
    /// (<c>{"S"|"N"|"BOOL"|"NULL"|"B"|"M"|"L"|"SS"|"NS"|"BS"}</c>), the shape the
    /// AWS SDK consumes — for SDK interop or migrating from KCL.
    /// </summary>
    DdbJson,
}
