using System.Security.Cryptography;
using System.Text;
using Grpc.Core;
using Grpc.Net.Client;
using PayBridge.Shared.Protos;

namespace PayBridge.PaymentApi.Services;

public interface IFraudClient
{
    Task<FraudCheckResponse> CheckAsync(Shared.Models.Payment payment, string customerEmail, CancellationToken ct = default);
}

public class FraudGrpcClient : IFraudClient
{
    private readonly FraudDetection.FraudDetectionClient _client;
    private readonly ILogger<FraudGrpcClient> _logger;

    public FraudGrpcClient(IConfiguration config, ILogger<FraudGrpcClient> logger)
    {
        _logger = logger;
        var address = config["FraudService:Address"] ?? "http://fraud-stub:8081";
        var channel = GrpcChannel.ForAddress(address);
        _client = new FraudDetection.FraudDetectionClient(channel);
    }

    public async Task<FraudCheckResponse> CheckAsync(
        Shared.Models.Payment payment,
        string customerEmail,
        CancellationToken ct = default)
    {
        // Hash PII before sending over the wire
        var emailHash = HashEmail(customerEmail);

        var request = new FraudCheckRequest
        {
            PaymentId         = payment.Id.ToString(),
            MerchantId        = payment.MerchantId,
            Amount            = (double)payment.Amount,
            Currency          = payment.Currency,
            CustomerEmailHash = emailHash,
            PaymentMethod     = payment.Method.ToString()
        };

        try
        {
            return await _client.CheckTransactionAsync(request,
                deadline: DateTime.UtcNow.AddSeconds(3),
                cancellationToken: ct);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
        {
            // Fail open — fraud service timeout should not block payment
            _logger.LogWarning("Fraud service timeout for payment {PaymentId} — failing open", payment.Id);
            return new FraudCheckResponse { Approved = true, RiskScore = 0.5, Reason = "timeout_fail_open" };
        }
        catch (RpcException ex)
        {
            _logger.LogError(ex, "Fraud service error for payment {PaymentId} — failing open", payment.Id);
            return new FraudCheckResponse { Approved = true, RiskScore = 0.5, Reason = "error_fail_open" };
        }
    }

    private static string HashEmail(string email)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(email.ToLowerInvariant()));
        return Convert.ToHexString(bytes)[..16]; // first 8 bytes for brevity
    }
}
