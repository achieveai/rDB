using System.Reflection;
using Google.Protobuf.Reflection;
using Pb = Rdb.V1;

namespace Rdb.Client.Tests;

/// <summary>
/// Each wire number's kind and rule, written out by hand from proto/rdb/v1/rdb.proto (review F-007). Not derived
/// from the enums under test, so swapping two members' numbers turns a row red.
/// </summary>
public class WireTableTests
{
    /// <summary>The proto's name for the generated member numbered <paramref name="wire"/>.</summary>
    private static string ProtoName<TEnum>(int wire) where TEnum : struct, Enum
    {
        var member = typeof(TEnum).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Single(f => Convert.ToInt32(f.GetValue(null)) == wire);
        return member.GetCustomAttribute<OriginalNameAttribute>()!.Name;
    }

    [Theory]
    [InlineData(1, "NOT_PRIMARY", RdbErrorKind.NotPrimary)]
    [InlineData(2, "ROUTE_CHANGED", RdbErrorKind.RouteChanged)]
    [InlineData(3, "LEASE_EXPIRED", RdbErrorKind.LeaseExpired)]
    [InlineData(4, "RECOVERY_READ_ONLY", RdbErrorKind.RecoveryReadOnly)]
    [InlineData(5, "PROTECTION_PAUSED", RdbErrorKind.ProtectionPaused)]
    [InlineData(6, "CONDITION_FAILED", RdbErrorKind.ConditionFailed)]
    [InlineData(7, "CROSS_AFFINITY", RdbErrorKind.CrossAffinity)]
    [InlineData(8, "INVALID_ARGUMENT", RdbErrorKind.InvalidArgument)]
    [InlineData(9, "OVERLOADED", RdbErrorKind.Overloaded)]
    [InlineData(10, "DEADLINE_BEFORE_ADMISSION", RdbErrorKind.DeadlineBeforeAdmission)]
    [InlineData(11, "UNKNOWN_OUTCOME", RdbErrorKind.UnknownOutcome)]
    [InlineData(12, "GENERATION_CHANGED", RdbErrorKind.GenerationChanged)]
    [InlineData(13, "REQUEST_ID_REUSE", RdbErrorKind.RequestIdReuse)]
    [InlineData(14, "STALE_CONTINUATION", RdbErrorKind.StaleContinuation)]
    [InlineData(15, "CORRUPT_HISTORY", RdbErrorKind.CorruptHistory)]
    [InlineData(16, "INCOMPATIBLE_VERSION", RdbErrorKind.IncompatibleVersion)]
    [InlineData(17, "STATUS_EXPIRED", RdbErrorKind.StatusExpired)]
    [InlineData(18, "DIVERGENCE_REQUIRES_OPERATOR", RdbErrorKind.DivergenceRequiresOperator)]
    [InlineData(19, "UNAVAILABLE", RdbErrorKind.Unavailable)]
    [InlineData(20, "UPLOAD_CHUNK_CONFLICT", RdbErrorKind.UploadChunkConflict)]
    [InlineData(21, "CORRUPT_BLOB", RdbErrorKind.CorruptBlob)]
    public void Each_wire_kind_number_is_its_proto_kind(int wire, string protoName, RdbErrorKind kind)
    {
        Assert.Equal(protoName, ProtoName<Pb.ErrorKind>(wire));
        Assert.Equal(kind, RdbErrors.Kind(new Pb.RdbError { Kind = (Pb.ErrorKind)wire }));
    }

    [Theory]
    [InlineData(1, "REFRESH_ROUTE", RdbRetry.RefreshRoute)]
    [InlineData(2, "RETRY_AFTER_RECOVERY", RdbRetry.RetryAfterRecovery)]
    [InlineData(3, "DEFINITIVE", RdbRetry.Definitive)]
    [InlineData(4, "BOUNDED_JITTER", RdbRetry.BoundedJitter)]
    [InlineData(5, "QUERY_STATUS", RdbRetry.QueryStatus)]
    [InlineData(6, "RECONCILE", RdbRetry.Reconcile)]
    [InlineData(7, "QUARANTINE", RdbRetry.Quarantine)]
    [InlineData(8, "NOT_WIRED", RdbRetry.NotWired)]
    public void Each_wire_rule_number_is_its_proto_rule(int wire, string protoName, RdbRetry rule)
    {
        Assert.Equal(protoName, ProtoName<Pb.RetryRule>(wire));
        Assert.Equal(rule, RdbErrors.Retry(new Pb.RdbError { Retry = (Pb.RetryRule)wire }));
    }

    [Fact]
    public void The_tables_list_every_kind_and_rule()
    {
        Assert.Equal(22, Enum.GetValues<RdbErrorKind>().Length); // 21 kinds and Unrecognized
        Assert.Equal(22, Enum.GetValues<Pb.ErrorKind>().Length);
        Assert.Equal(9, Enum.GetValues<RdbRetry>().Length);
        Assert.Equal(9, Enum.GetValues<Pb.RetryRule>().Length);
    }
}
