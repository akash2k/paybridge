namespace PayBridge.Shared.Models;

public class Payment
{
    public Guid Id { get; set; }
    public string MerchantId { get; set; } = default!;
    public string TenantId { get; set; } = default!;
    public string IdempotencyKey { get; set; } = default!;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = default!;
    public PaymentMethod Method { get; set; }
    public PaymentStatus Status { get; set; }
    public string? ProviderTransactionId { get; set; }
    public string? FailureReason { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public string? OriginalTraceId { get; set; }
}

public enum PaymentMethod { CreditCard, DebitCard, BankTransfer, Wallet }

public enum PaymentStatus { Created, FraudChecking, Submitted, Completed, Failed, Refunded }

public record CreatePaymentRequest(
    string MerchantId,
    string IdempotencyKey,
    decimal Amount,
    string Currency,
    string CustomerEmail,
    PaymentMethod Method,
    Dictionary<string, string>? Metadata
);

public record PaymentResponse(
    Guid PaymentId,
    PaymentStatus Status,
    string? ProviderTransactionId,
    string? FailureReason,
    DateTime CreatedAt
);
