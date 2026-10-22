using System.Net.Http.Json;
using KafkaEdaStudy.Contracts;

namespace KafkaEdaStudy.Playground.Scenarios;

public sealed record PlaceOrderRequest(Guid CustomerId, IReadOnlyList<OrderLine> Lines);

public sealed record PlaceOrderResponse(Guid OrderId, Guid CorrelationId, string Status);

/// <summary>Typed HttpClient for Orders.Api (base address from Services:OrdersApi).</summary>
public sealed class OrdersApiClient(HttpClient http)
{
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
