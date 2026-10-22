using System.Text.Json.Serialization;

namespace KafkaEdaStudy.Playground.Scenarios;

/// <summary>A button in the playground UI that triggers a business flow by calling a service.</summary>
public interface IScenario
{
    string Name { get; }
    string Title { get; }
    string Description { get; }
    bool Available { get; }

    /// <param name="input">Optional options from the request body; never null (empty when no body was sent).</param>
    Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken);
}

/// <summary>
/// Optional body of <c>POST /api/scenarios/{name}</c>. Every field is optional and each scenario
/// reads only what it understands: <c>OrderId</c> targets an existing order, <c>Kind</c> picks a variant.
/// </summary>
public sealed record ScenarioInput(string? OrderId = null, string? Kind = null)
{
    public static readonly ScenarioInput Empty = new();
}

public sealed record ScenarioInfo(string Name, string Title, string Description, bool Available);

public sealed record ScenarioResult(
    string Scenario,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? OrderId,
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? CorrelationId,
    string Message);

/// <summary>Thrown when the downstream service call fails; mapped to 502 Bad Gateway.</summary>
public sealed class DownstreamException(string message, Exception? inner = null) : Exception(message, inner);

/// <summary>
/// The scenario cannot run with this input: bad options (400) or nothing to act on (404).
/// The message is shown to the user as is.
/// </summary>
public sealed class ScenarioRejectedException(int statusCode, string message) : Exception(message)
{
    public int StatusCode { get; } = statusCode;

    public static ScenarioRejectedException BadRequest(string message) => new(StatusCodes.Status400BadRequest, message);

    public static ScenarioRejectedException NotFound(string message) => new(StatusCodes.Status404NotFound, message);

    public static ScenarioRejectedException Conflict(string message) => new(StatusCodes.Status409Conflict, message);
}

/// <summary>Listed in the UI (disabled) until a later roadmap step implements it.</summary>
public sealed record PlannedScenario(string Name, string Title, string Description) : IScenario
{
    public bool Available => false;

    public Task<ScenarioResult> RunAsync(ScenarioInput input, CancellationToken cancellationToken) =>
        throw new NotSupportedException($"Scenario '{Name}' is not implemented yet.");
}
