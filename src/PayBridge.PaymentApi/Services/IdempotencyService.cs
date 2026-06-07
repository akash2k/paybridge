using System.Text.Json;
using StackExchange.Redis;

namespace PayBridge.PaymentApi.Services;

public interface IIdempotencyService
{
    Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class;
    Task SetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default) where T : class;
    Task SetTraceAsync(Guid paymentId, string traceId, CancellationToken ct = default);
    Task<string?> GetTraceAsync(Guid paymentId, CancellationToken ct = default);
    Task<bool> SetIfNotExistsAsync(string key, TimeSpan? ttl = null, CancellationToken ct = default);
}

public class RedisIdempotencyService : IIdempotencyService
{
    private readonly IDatabase _db;
    private readonly ILogger<RedisIdempotencyService> _logger;
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromHours(24);

    public RedisIdempotencyService(IConnectionMultiplexer redis, ILogger<RedisIdempotencyService> logger)
    {
        _db = redis.GetDatabase();
        _logger = logger;
    }

    public async Task<T?> GetAsync<T>(string key, CancellationToken ct = default) where T : class
    {
        var value = await _db.StringGetAsync(key);
        if (value.IsNullOrEmpty) return null;
        return JsonSerializer.Deserialize<T>(value!);
    }

    public async Task SetAsync<T>(string key, T value, TimeSpan? ttl = null, CancellationToken ct = default) where T : class
    {
        var json = JsonSerializer.Serialize(value);
        await _db.StringSetAsync(key, json, ttl ?? DefaultTtl);
    }

    public async Task SetTraceAsync(Guid paymentId, string traceId, CancellationToken ct = default)
    {
        await _db.StringSetAsync($"trace:{paymentId}", traceId, TimeSpan.FromHours(48));
    }

    public async Task<string?> GetTraceAsync(Guid paymentId, CancellationToken ct = default)
    {
        var val = await _db.StringGetAsync($"trace:{paymentId}");
        return val.IsNullOrEmpty ? null : val.ToString();
    }

    public async Task<bool> SetIfNotExistsAsync(string key, TimeSpan? ttl = null, CancellationToken ct = default)
    {
        return await _db.StringSetAsync(key, "1", ttl ?? DefaultTtl, When.NotExists);
    }
}
