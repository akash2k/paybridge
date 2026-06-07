namespace PayBridge.Shared.Events;

public record PaymentEvent(
    Guid PaymentId,
    string MerchantId,
    string TenantId,
    string EventType,   // "PaymentInitiated" | "PaymentCompleted" | "PaymentFailed"
    decimal Amount,
    string Currency,
    string? ProviderTransactionId,
    string? FailureReason,
    DateTime Timestamp
);

public record ProviderWebhookCallback(
    string ProviderTransactionId,
    string Status,          // "SUCCESS" | "FAILURE"
    DateTime Timestamp,
    Dictionary<string, string> Metadata
);
