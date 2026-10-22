using System.Net;
using System.Net.Http.Json;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Scenarios;

public sealed record ShipmentInfo(Guid Id, Guid OrderId, string Status, string? TrackingCode);

/// <summary>Typed HttpClient for Shipping.Api (base address from Services:ShippingApi).</summary>
public sealed class ShippingApiClient(HttpClient http)
{
    /// <summary>Shipments, newest first, optionally only those in one status (Created, Dispatched, Delivered).</summary>
    public async Task<IReadOnlyList<ShipmentInfo>> ListShipmentsAsync(string? status, int limit, CancellationToken cancellationToken)
    {
        var query = $"shipments?limit={limit}" + (status is null ? "" : $"&status={status}");
        using var response = await http.GetAsync(query, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<ShipmentInfo>>(EventJson.Options, cancellationToken) ?? [];
    }

    /// <summary>Rule S2: the parcel is handed to the carrier.</summary>
    public Task<ShipmentInfo> DispatchAsync(Guid orderId, CancellationToken cancellationToken) =>
        PostAsync($"shipments/{orderId}/dispatch", cancellationToken);

    /// <summary>Rule S2: the carrier reports the parcel delivered.</summary>
    public Task<ShipmentInfo> DeliverAsync(Guid orderId, CancellationToken cancellationToken) =>
        PostAsync($"shipments/{orderId}/deliver", cancellationToken);

    private async Task<ShipmentInfo> PostAsync(string path, CancellationToken cancellationToken)
    {
        using var response = await http.PostAsync(path, null, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<ShipmentInfo>(EventJson.Options, cancellationToken)
            ?? throw new DownstreamException("Shipping.Api returned an empty response.");
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var message = $"Shipping.Api returned {(int)response.StatusCode} {response.ReasonPhrase}: {body}";
        throw response.StatusCode switch
        {
            HttpStatusCode.NotFound => ScenarioRejectedException.NotFound(message),
            HttpStatusCode.Conflict => ScenarioRejectedException.Conflict(message),
            _ => new DownstreamException(message),
        };
    }
}
