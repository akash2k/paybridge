using System.Text.Json;
using PayBridge.Shared.Models;

namespace PayBridge.PaymentApi.Services;

public interface IProviderClient
{
    Task<ProviderSubmitResult> SubmitAsync(Payment payment, CancellationToken ct = default);
}

public record ProviderSubmitResult(bool Accepted, string ProviderTransactionId, string? ErrorMessage);

public class ProviderHttpClient : IProviderClient
{
    private readonly HttpClient _http;
    private readonly ILogger<ProviderHttpClient> _logger;

    public ProviderHttpClient(HttpClient http, ILogger<ProviderHttpClient> logger)
    {
        _http = http;
        _logger = logger;
    }

    public async Task<ProviderSubmitResult> SubmitAsync(Payment payment, CancellationToken ct = default)
    {
        var payload = new
        {
            payment_id  = payment.Id.ToString(),
            merchant_id = payment.MerchantId,
            amount      = payment.Amount,
            currency    = payment.Currency,
            method      = payment.Method.ToString()
        };

        try
        {
            var response = await _http.PostAsJsonAsync("/payments/submit", payload, ct);
            response.EnsureSuccessStatusCode();

            var body = await response.Content.ReadFromJsonAsync<ProviderResponse>(cancellationToken: ct);
            return new ProviderSubmitResult(true, body!.TransactionId, null);
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "Provider HTTP error for payment {PaymentId}", payment.Id);
            return new ProviderSubmitResult(false, string.Empty, ex.Message);
        }
    }

    private record ProviderResponse(string TransactionId);
}
