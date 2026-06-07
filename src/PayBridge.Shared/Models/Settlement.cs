using PayBridge.Shared.Models;

namespace PayBridge.Shared.Models;

public class SettlementRecord
{
    public long Id { get; set; }
    public Guid PaymentId { get; set; }
    public string MerchantId { get; set; } = default!;
    public string TenantId { get; set; } = default!;
    public decimal Amount { get; set; }
    public string Currency { get; set; } = default!;
    public PaymentStatus FinalStatus { get; set; }
    public string? ProviderTransactionId { get; set; }
    public DateTime EventTimestamp { get; set; }
    public DateTime PersistedAt { get; set; }
}

public class OutboxEvent
{
    public long Id { get; set; }
    public string Topic { get; set; } = default!;
    public string Payload { get; set; } = default!;
    public string? TraceContext { get; set; }
    public DateTime CreatedAt { get; set; }
    public int RetryCount { get; set; }
    public DateTime? ProcessedAt { get; set; }
}
