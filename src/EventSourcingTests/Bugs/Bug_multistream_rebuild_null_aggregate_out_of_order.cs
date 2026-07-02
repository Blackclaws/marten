using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using JasperFx.Events;
using JasperFx.Events.Projections;
using Marten;
using Marten.Events.Projections;
using Marten.Testing.Harness;
using Npgsql;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace EventSourcingTests.Bugs;

// Repro for: MultiStreamProjection rebuild produces a blank/uninitialized aggregate
// (Create handler skipped) when a cross-stream "apply-type" event is assigned a LOWER
// global seq_id than the aggregate's "create-type" event.
//
// The lower seq_id happens because Marten assigns global sequence numbers in
// first-touch stream order within a single SaveChangesAsync. If the "tagger" stream is
// touched before the doc stream, the DocTagged event (apply-only) gets a lower seq_id
// than DocCreated (create). On rebuild, events for a slice are applied in global seq
// order: DocTagged first -> a blank Doc is auto-materialized -> DocCreated arrives with
// a non-null snapshot -> Create is NOT invoked -> Name is lost.

#region repro types

public record DocCreated(string DocId, string Name);

public record DocTagged(string DocId, string Tag);

public class Doc
{
    public string Id { get; set; } = default!;
    public string Name { get; set; } = default!;
    public System.Collections.Generic.List<string> Tags { get; set; } = new();
}

public partial class DocProjection: MultiStreamProjection<Doc, string>
{
    public DocProjection()
    {
        Identity<DocCreated>(e => e.DocId);
        Identity<DocTagged>(e => e.DocId);
    }

    // create-type event: has a Create handler, no Apply overload
    public static Doc Create(DocCreated e) => new Doc { Id = e.DocId, Name = e.Name };

    // apply-type event: has an Apply handler, NO Create handler
    public void Apply(DocTagged e, Doc doc) => doc.Tags.Add(e.Tag);
}

#endregion

#region 42883 repro types

// A Vogen-style [ValueObject<string>] stand-in WITHOUT taking a Vogen dependency.
// The only way to set _initialized = true is the real constructor. Marten builds
// the blank/out-of-order aggregate via GetUninitializedObject (see the null-aggregate
// bug above), which does NOT run any constructor, so on a blank Doc this struct is the
// default value with _initialized == false. Its JsonConverter.Write THROWS in that
// state -> System.Text.Json serialization of the blank aggregate throws.
[JsonConverter(typeof(RequiredNameConverter))]
public readonly struct RequiredName
{
    private readonly bool _initialized;

    public string Value { get; }

    // The ONLY path that sets _initialized. default(RequiredName) and
    // RuntimeHelpers.GetUninitializedObject(...) both leave it false.
    public RequiredName(string value)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value));
        _initialized = true;
    }

    public bool IsInitialized => _initialized;
}

public sealed class RequiredNameConverter: JsonConverter<RequiredName>
{
    public override RequiredName Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => new(reader.GetString() ?? throw new InvalidOperationException("null RequiredName in JSON"));

    public override void Write(Utf8JsonWriter writer, RequiredName value, JsonSerializerOptions options)
    {
        // Mirrors a Vogen struct's serialization guard: an uninitialized value object
        // must never be persisted. THIS is what throws mid-store-bind for the blank aggregate.
        if (!value.IsInitialized)
        {
            throw new InvalidOperationException(
                "Cannot serialize an uninitialized RequiredName value object");
        }

        writer.WriteStringValue(value.Value);
    }
}

// Same shape as Doc, but carries the throwing value object that Create sets and
// Apply never touches. On out-of-order rebuild the blank aggregate is materialized
// with an uninitialized RequiredName -> serialize throws.
public class RevDoc
{
    public string Id { get; set; } = default!;

    // Uninitialized on the blank aggregate -> serialization throws.
    public RequiredName Name { get; set; }

    public System.Collections.Generic.List<string> Tags { get; set; } = new();
}

public partial class RevDocProjection: MultiStreamProjection<RevDoc, string>
{
    public RevDocProjection()
    {
        Identity<DocCreated>(e => e.DocId);
        Identity<DocTagged>(e => e.DocId);
    }

    // create-type event: sets the value object to a REAL (initialized) value.
    public static RevDoc Create(DocCreated e) =>
        new RevDoc { Id = e.DocId, Name = new RequiredName(e.Name) };

    // apply-type event: never touches Name -> the blank aggregate keeps its
    // uninitialized RequiredName all the way to the upsert bind.
    public void Apply(DocTagged e, RevDoc doc) => doc.Tags.Add(e.Tag);
}

#endregion

public class Bug_multistream_rebuild_null_aggregate_out_of_order: OneOffConfigurationsContext
{
    private readonly ITestOutputHelper _output;

    public Bug_multistream_rebuild_null_aggregate_out_of_order(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task rebuild_should_not_produce_blank_aggregate_when_apply_event_precedes_create_event()
    {
        StoreOptions(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<DocProjection>(ProjectionLifecycle.Async);
        });

        const string docId = "doc-1";

        // ONE session. Touch the "tagger" stream FIRST so DocTagged gets a lower seq_id,
        // THEN start the doc stream with DocCreated. Code order is irrelevant; first-touch
        // stream order drives global seq_id assignment.
        theSession.Events.Append("tagger", new DocTagged(docId, "important"));
        theSession.Events.StartStream(docId, new DocCreated(docId, "The Real Name"));
        await theSession.SaveChangesAsync();

        // Prove the seq inversion: DocTagged.Sequence < DocCreated.Sequence
        var allEvents = await theSession.Events.QueryAllRawEvents().ToListAsync();
        var taggedSeq = allEvents.Single(e => e.Data is DocTagged).Sequence;
        var createdSeq = allEvents.Single(e => e.Data is DocCreated).Sequence;

        _output.WriteLine($"DocTagged.Sequence  = {taggedSeq}");
        _output.WriteLine($"DocCreated.Sequence = {createdSeq}");

        taggedSeq.ShouldBeLessThan(createdSeq,
            "Precondition: the apply-type event must land at a lower global seq than the create-type event");

        // Rebuild the projection from scratch via the async daemon (same pattern as Marten's own rebuild tests)
        using var daemon = await theStore.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync<Doc>(CancellationToken.None);

        // What a user expects: the created Name survives and the tag is applied.
        var doc = await theSession.LoadAsync<Doc>(docId);

        doc.ShouldNotBeNull();
        _output.WriteLine($"Post-rebuild Doc.Name = '{doc!.Name ?? "<null>"}', Tags = [{string.Join(",", doc.Tags)}]");

        doc.Name.ShouldBe("The Real Name"); // FAILS on master: Name is null (Create was skipped)
        doc.Tags.ShouldContain("important");
    }

    // Control: identical projection, but the doc stream is touched FIRST so DocCreated
    // gets the lower seq. This passes on master, isolating event ordering as the cause.
    [Fact]
    public async Task control_rebuild_succeeds_when_create_event_precedes_apply_event()
    {
        StoreOptions(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<DocProjection>(ProjectionLifecycle.Async);
        });

        const string docId = "doc-1";

        // Doc stream touched first -> DocCreated gets the lower seq.
        theSession.Events.StartStream(docId, new DocCreated(docId, "The Real Name"));
        theSession.Events.Append("tagger", new DocTagged(docId, "important"));
        await theSession.SaveChangesAsync();

        var allEvents = await theSession.Events.QueryAllRawEvents().ToListAsync();
        var taggedSeq = allEvents.Single(e => e.Data is DocTagged).Sequence;
        var createdSeq = allEvents.Single(e => e.Data is DocCreated).Sequence;
        _output.WriteLine($"DocCreated.Sequence = {createdSeq}, DocTagged.Sequence = {taggedSeq}");
        createdSeq.ShouldBeLessThan(taggedSeq);

        using var daemon = await theStore.BuildProjectionDaemonAsync();
        await daemon.RebuildProjectionAsync<Doc>(CancellationToken.None);

        var doc = await theSession.LoadAsync<Doc>(docId);
        doc.ShouldNotBeNull();
        doc!.Name.ShouldBe("The Real Name");
        doc.Tags.ShouldContain("important");
    }

    // The REAL-system failure mode. Same out-of-order rebuild as the first test, but the
    // aggregate carries a value-object struct that THROWS during STJ serialization when it
    // is the default/uninitialized struct (what Marten's GetUninitializedObject produces for
    // the blank aggregate). Because RevDoc is a MultiStreamProjection document, Marten forces
    // numeric revisions (ProjectionDocumentPolicy) -> the persist is the closed-shape numeric
    // upsert whose VALUES emits `mt_version = CASE WHEN ? = 0 THEN 1 ELSE ? END`. The upsert
    // binds id, then serializes the document into the data param, THEN binds the revision
    // params. When serialization throws mid-bind the revision param is never rebound and stays
    // at Weasel's Text placeholder seed -> Postgres parses `text = integer` at `CASE WHEN $3 = 0`
    // -> 42883 -> the whole daemon store batch rolls back and the projection never completes.
    //
    // NOTE on the daemon entry point: RebuildProjectionAsync in JasperFx.Events 2.18.1 does NOT
    // rethrow a store failure to its caller — it records the shard error and returns (leaving a
    // silent gap: zero rows persisted). CatchUpAsync DOES rethrow, via
    // JasperFxAsyncDaemon.throwIfRecordedExceptions. Both drive the identical out-of-order
    // store; CatchUpAsync is the one that surfaces the real 42883 to assert on.
    [Fact]
    public async Task daemon_store_throws_postgres_42883_when_blank_aggregate_serialization_fails_mid_bind()
    {
        var logger = new TestOutputMartenLogger(_output);

        StoreOptions(opts =>
        {
            opts.Events.StreamIdentity = StreamIdentity.AsString;
            opts.Projections.Add<RevDocProjection>(ProjectionLifecycle.Async);
            opts.Logger(logger);

            // Do not let the daemon skip the failing slice — surface the underlying
            // store exception (JasperFx.Events 2.0 default is skip = true).
            opts.Projections.Errors.SkipApplyErrors = false;
            opts.Projections.Errors.SkipSerializationErrors = false;
            opts.Projections.RebuildErrors.SkipApplyErrors = false;
            opts.Projections.RebuildErrors.SkipSerializationErrors = false;
        });

        const string docId = "rev-doc-1";

        // Same first-touch inversion: DocTagged (apply-only) lands at a lower global seq than
        // DocCreated (create), so the daemon materializes a BLANK RevDoc first.
        theSession.Events.Append("tagger", new DocTagged(docId, "important"));
        theSession.Events.StartStream(docId, new DocCreated(docId, "The Real Name"));
        await theSession.SaveChangesAsync();

        var allEvents = await theSession.Events.QueryAllRawEvents().ToListAsync();
        var taggedSeq = allEvents.Single(e => e.Data is DocTagged).Sequence;
        var createdSeq = allEvents.Single(e => e.Data is DocCreated).Sequence;
        _output.WriteLine($"DocTagged.Sequence  = {taggedSeq}");
        _output.WriteLine($"DocCreated.Sequence = {createdSeq}");
        taggedSeq.ShouldBeLessThan(createdSeq,
            "Precondition: the apply-type event must land at a lower global seq than the create-type event");

        using var daemon = await theStore.BuildProjectionDaemonAsync();

        // CatchUpAsync wraps the store failure in AggregateException -> MartenCommandException
        // -> PostgresException 42883.
        var aggregate = await Should.ThrowAsync<AggregateException>(async () =>
            await daemon.CatchUpAsync(CancellationToken.None));

        _output.WriteLine("=== CatchUp exception chain ===");
        PostgresException pg = null;
        for (Exception current = aggregate; current is not null; current = current.InnerException)
        {
            _output.WriteLine($"{current.GetType().FullName}: {current.Message}");
            if (current is PostgresException candidate)
            {
                pg = candidate;
            }
        }

        _output.WriteLine("=== executed SQL ===");
        _output.WriteLine(logger.ExecutedSql());

        pg.ShouldNotBeNull("Expected a Npgsql.PostgresException in the store-failure chain");
        _output.WriteLine($"SqlState = {pg!.SqlState}, Position = {pg.Position}");

        pg.SqlState.ShouldBe("42883");
        pg.MessageText.ShouldContain("operator does not exist: text = integer");
    }

    // Diagnostic: confirm the blank/uninitialized aggregate DOES throw on STJ serialize.
    [Fact]
    public void blank_revdoc_serialization_throws()
    {
        var blank = (RevDoc)System.Runtime.CompilerServices.RuntimeHelpers
            .GetUninitializedObject(typeof(RevDoc));
        blank.Id = "x";
        _output.WriteLine($"blank.Name.IsInitialized = {blank.Name.IsInitialized}");

        var ex = Should.Throw<Exception>(() => JsonSerializer.Serialize(blank));
        _output.WriteLine($"Serialize threw: {ex.GetType().FullName}: {ex.Message}");
    }
}
