namespace Amazon.DynamoDBStreams.Consumer;

/// <summary>
/// A DynamoDB Streams change record delivered to an <see cref="IRecordProcessor"/>.
/// </summary>
/// <remarks>
/// Item images (<see cref="Keys"/>, <see cref="NewImage"/>, <see cref="OldImage"/>)
/// are decoded according to the worker's <see cref="RecordFormat"/>:
/// <list type="bullet">
///   <item><see cref="RecordFormat.Native"/> — values are plain .NET objects
///   (<see cref="string"/>, <see cref="bool"/>, <c>null</c>, <see cref="byte"/>[],
///   <see cref="System.Collections.Generic.List{T}"/>, and nested
///   <c>IReadOnlyDictionary&lt;string, object?&gt;</c>).</item>
///   <item><see cref="RecordFormat.DdbJson"/> — each value is a single-entry
///   dictionary in canonical DynamoDB JSON form (e.g. <c>{"S": "x"}</c>).</item>
/// </list>
/// </remarks>
public sealed class Record
{
    /// <summary>The shard this record was delivered from.</summary>
    public string ShardId { get; init; } = "";

    /// <summary>INSERT / MODIFY / REMOVE.</summary>
    public string? EventName { get; init; }

    /// <summary>The record's sequence number.</summary>
    public string? SequenceNumber { get; init; }

    /// <summary>KEYS_ONLY / NEW_IMAGE / OLD_IMAGE / NEW_AND_OLD_IMAGES.</summary>
    public string? StreamViewType { get; init; }

    /// <summary>The key attributes of the changed item.</summary>
    public IReadOnlyDictionary<string, object?> Keys { get; init; } =
        new Dictionary<string, object?>();

    /// <summary>The item image after the change, when present.</summary>
    public IReadOnlyDictionary<string, object?>? NewImage { get; init; }

    /// <summary>The item image before the change, when present.</summary>
    public IReadOnlyDictionary<string, object?>? OldImage { get; init; }
}

/// <summary>
/// Customer business logic: called with ordered batches for a single shard.
/// </summary>
public interface IRecordProcessor
{
    /// <summary>
    /// Deliver a batch of records, already in per-shard sequence order. Returning
    /// normally acknowledges the batch, advancing the durable checkpoint to its
    /// last record (at-least-once).
    /// </summary>
    void ProcessRecords(IReadOnlyList<Record> records);

    /// <summary>Called when the shard reaches SHARD_END. Default: no-op.</summary>
    void ShardEnded(string shardId) { }
}
