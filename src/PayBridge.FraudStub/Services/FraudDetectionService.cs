using Grpc.Core;
using PayBridge.Shared.Protos;

namespace PayBridge.FraudStub.Services;

public class FraudDetectionService : FraudDetection.FraudDetectionBase
{
    private readonly ILogger<FraudDetectionService> _logger;
    private static readonly Random Rng = new();

    public FraudDetectionService(ILogger<FraudDetectionService> logger)
    {
        _logger = logger;
    }

    public override async Task<FraudCheckResponse> CheckTransaction(
        FraudCheckRequest request,
        ServerCallContext context)
    {
        // Simulate processing time (20-150ms)
        await Task.Delay(Rng.Next(20, 150), context.CancellationToken);

        // 5% rejection rate, random risk score
        var riskScore = Rng.NextDouble();
        var approved  = riskScore < 0.80;   // top 20% risk scores are rejected

        var reason = approved
            ? riskScore < 0.3 ? "low_risk"
            : riskScore < 0.6 ? "medium_risk"
            : "elevated_risk"
            : "high_risk_rejected";

        _logger.LogInformation(
            "Fraud check for payment {PaymentId}: riskScore={Score:F3} approved={Approved}",
            request.PaymentId, riskScore, approved);

        return new FraudCheckResponse
        {
            Approved  = approved,
            RiskScore = riskScore,
            Reason    = reason
        };
    }
}
