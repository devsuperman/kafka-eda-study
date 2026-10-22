using System.Net.Http.Json;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Scenarios;

public sealed record PlaceOrderRequest(Guid CustomerId, IReadOnlyList<OrderLine> Lines);

public sealed record PlaceOrderResponse(Guid OrderId, Guid CorrelationId, string Status);

public sealed record OrderInfo(Guid Id, string Status, decimal Total);

/// <summary>Typed HttpClient for Orders.Api (base address from Services:OrdersApi).</summary>
public sealed class OrdersApiClient(HttpClient http)
{
    /// <summary>The most recent orders, newest first.</summary>
    public async Task<IReadOnlyList<OrderInfo>> ListOrdersAsync(int limit, CancellationToken cancellationToken)
    {
        using var response = await http.GetAsync($"orders?limit={limit}", cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync<List<OrderInfo>>(EventJson.Options, cancellationToken) ?? [];
    }

    /// <summary>Rule O6: the customer cancels an order that has not shipped yet.</summary>
    public async Task CancelOrderAsync(Guid orderId, CancellationToken cancellationToken)
    {
        using var response = await http.PostAsync($"orders/{orderId}/cancel", null, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;

        var body = await response.Content.ReadAsStringAsync(cancellationToken);
        var message = $"Orders.Api returned {(int)response.StatusCode} {response.ReasonPhrase}: {body}";
        throw response.StatusCode switch
        {
            System.Net.HttpStatusCode.NotFound => ScenarioRejectedException.NotFound(message),
            System.Net.HttpStatusCode.Conflict => ScenarioRejectedException.Conflict(message),
            _ => new DownstreamException(message),
        };
    }

    public async Task<PlaceOrderResponse> PlaceOrderAsync(PlaceOrderRequest request, CancellationToken cancellationToken)
    {
        HttpResponseMessage response;
        try
        {
            // Buffer the small body so it is sent with Content-Length instead of chunked.
            using var content = JsonContent.Create(request, options: EventJson.Options);
            await content.LoadIntoBufferAsync(cancellationToken);
            response = await http.PostAsync("orders", content, cancellationToken);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !cancellationToken.IsCancellationRequested)
        {
            throw new DownstreamException($"Orders.Api at {http.BaseAddress} is unreachable: {ex.Message}", ex);
        }

        using (response)
        {
            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(cancellationToken);
                throw new DownstreamException(
                    $"Orders.Api returned {(int)response.StatusCode} {response.ReasonPhrase}: {body}");
            }

            return await response.Content.ReadFromJsonAsync<PlaceOrderResponse>(EventJson.Options, cancellationToken)
                ?? throw new DownstreamException("Orders.Api returned an empty response.");
        }
    }
}
