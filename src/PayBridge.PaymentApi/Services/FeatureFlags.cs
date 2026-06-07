using StackExchange.Redis;

namespace PayBridge.PaymentApi.Services;

public interface IFeatureFlags
{
    Task<bool> IsPaymentProcessingEnabledAsync();
}

public class RedisFeatureFlags : IFeatureFlags
{
    private readonly IDatabase _db;
    private bool _cached = true;
    private DateTime _cacheExpiry = DateTime.MinValue;

    public RedisFeatureFlags(IConnectionMultiplexer redis)
    {
        _db = redis.GetDatabase();
    }

    public async Task<bool> IsPaymentProcessingEnabledAsync()
    {
        // Cache for 30 seconds to avoid Redis round-trip on every request
        if (DateTime.UtcNow < _cacheExpiry) return _cached;

        var value = await _db.StringGetAsync("flags:payment_processing");
        _cached = value.IsNullOrEmpty || value.ToString() != "false";
        _cacheExpiry = DateTime.UtcNow.AddSeconds(30);
        return _cached;
    }
}
