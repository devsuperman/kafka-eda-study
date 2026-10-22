using KafkaEdaStudy.Contracts;
using KafkaEdaStudy.Payments.Domain;

namespace KafkaEdaStudy.Payments.UnitTests;

/// <summary>Rules P1 (authorize on placed, capture on shipped), P2 (limit), P3 (void before capture), P4 (once per order).</summary>
public sealed class AuthorizationTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void A_total_up_to_the_limit_is_authorized()
    {
        var authorization = Authorization.Decide(Guid.NewGuid(), Authorization.Limit, Now);

        Assert.Equal(AuthorizationStatus.Authorized, authorization.Status);
        Assert.Null(authorization.DeclineReason);
    }

    [Fact]
    public void A_total_above_the_limit_is_declined_with_a_reason()
    {
        var authorization = Authorization.Decide(Guid.NewGuid(), 1500m, Now);

        Assert.Equal(AuthorizationStatus.Declined, authorization.Status);
        Assert.Equal(DeclineReasons.OverLimit, authorization.DeclineReason);
    }

    [Fact]
    public void Void_works_only_before_capture()
    {
        var authorization = Authorization.Decide(Guid.NewGuid(), 100m, Now);

        Assert.True(authorization.Void(Now));
        Assert.Equal(AuthorizationStatus.Voided, authorization.Status);
        Assert.False(authorization.Capture(Now));
    }

    [Fact]
    public void Capture_happens_once()
    {
        var authorization = Authorization.Decide(Guid.NewGuid(), 100m, Now);

        Assert.True(authorization.Capture(Now));
        Assert.False(authorization.Capture(Now));
        Assert.False(authorization.Void(Now));
        Assert.Equal(AuthorizationStatus.Captured, authorization.Status);
    }

    [Fact]
    public void A_declined_authorization_cannot_be_captured_or_voided()
    {
        var authorization = Authorization.Decide(Guid.NewGuid(), 1500m, Now);

        Assert.False(authorization.Capture(Now));
        Assert.False(authorization.Void(Now));
    }
}
