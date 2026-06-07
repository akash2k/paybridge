using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Http.Resilience;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using PayBridge.PaymentApi.Infrastructure;
using PayBridge.PaymentApi.Services;
using Polly;
using Serilog;
using Serilog.Events;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// ─── Serilog ────────────────────────────────────────────────────────────────
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft.EntityFrameworkCore", LogEventLevel.Warning)
    .Enrich.FromLogContext()
    .Enrich.WithMachineName()
    .WriteTo.Console(outputTemplate:
        "[{Timestamp:HH:mm:ss} {Level:u3}] {Message:lj} " +
        "{Properties:j}{NewLine}{Exception}")
    .CreateLogger();

builder.Host.UseSerilog();

// ─── Services ────────────────────────────────────────────────────────────────
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(
        new System.Text.Json.Serialization.JsonStringEnumConverter()));
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Database
builder.Services.AddDbContext<AppDbContext>(opt =>
    opt.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

// Redis
var redisConn = builder.Configuration["Redis:ConnectionString"] ?? "redis:6379";
builder.Services.AddSingleton<IConnectionMultiplexer>(
    ConnectionMultiplexer.Connect(redisConn));

// Provider HTTP client with resilience pipeline
builder.Services.AddHttpClient<IProviderClient, ProviderHttpClient>(client =>
{
    client.BaseAddress = new Uri(builder.Configuration["ProviderStub:BaseUrl"] ?? "http://provider-stub:8082");
    client.Timeout = TimeSpan.FromSeconds(10);
})
.AddResilienceHandler("provider-pipeline", pipeline =>
{
    pipeline.AddRetry(new HttpRetryStrategyOptions
    {
        MaxRetryAttempts = 3,
        BackoffType      = DelayBackoffType.Exponential,
        UseJitter        = true,
        Delay            = TimeSpan.FromMilliseconds(200),
        ShouldHandle     = args => ValueTask.FromResult(
            args.Outcome.Result?.IsSuccessStatusCode == false ||
            args.Outcome.Exception is HttpRequestException)
    });

    pipeline.AddCircuitBreaker(new HttpCircuitBreakerStrategyOptions
    {
        FailureRatio       = 0.5,
        SamplingDuration   = TimeSpan.FromSeconds(30),
        MinimumThroughput  = 5,
        BreakDuration      = TimeSpan.FromSeconds(60),
        OnOpened           = args =>
        {
            Log.Warning("Circuit breaker OPENED for payment provider — {Reason}", args.Outcome.Exception?.Message);
            return ValueTask.CompletedTask;
        },
        OnClosed = _ =>
        {
            Log.Information("Circuit breaker CLOSED — provider recovered");
            return ValueTask.CompletedTask;
        }
    });

    pipeline.AddTimeout(TimeSpan.FromSeconds(8));
});

// Application services
builder.Services.AddScoped<IFraudClient, FraudGrpcClient>();
builder.Services.AddScoped<IIdempotencyService, RedisIdempotencyService>();
builder.Services.AddScoped<IFeatureFlags, RedisFeatureFlags>();
builder.Services.AddScoped<IKafkaProducer, KafkaProducer>();
builder.Services.AddScoped<PaymentService>();
builder.Services.AddHostedService<OutboxWorker>();

// ─── OpenTelemetry ───────────────────────────────────────────────────────────
var otelEndpoint = builder.Configuration["Otel:Endpoint"] ?? "http://otel-collector:4317";

builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r
        .AddService("PayBridge.PaymentApi")
        .AddAttributes(new Dictionary<string, object>
        {
            ["deployment.environment"] = builder.Environment.EnvironmentName
        }))
    .WithTracing(tracing => tracing
        .AddSource("PayBridge.PaymentApi")
        .AddAspNetCoreInstrumentation(opt =>
        {
            opt.RecordException = true;
            opt.Filter = ctx => !ctx.Request.Path.StartsWithSegments("/health");
        })
        .AddHttpClientInstrumentation()
        .AddGrpcClientInstrumentation()
        .AddEntityFrameworkCoreInstrumentation(opt => opt.SetDbStatementForText = true)
        .AddOtlpExporter(opt => opt.Endpoint = new Uri(otelEndpoint)))
    .WithMetrics(metrics => metrics
        .AddMeter("PayBridge.PaymentApi")
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddRuntimeInstrumentation()
        .AddOtlpExporter(opt => opt.Endpoint = new Uri(otelEndpoint)));

// ─── Health Checks ────────────────────────────────────────────────────────────
builder.Services.AddHealthChecks()
    .AddNpgSql(
        builder.Configuration.GetConnectionString("Postgres")!,
        name: "postgresql",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["critical", "ready"])
    .AddRedis(
        redisConn,
        name: "redis",
        failureStatus: HealthStatus.Unhealthy,
        tags: ["critical", "ready"])
    .AddCheck("fraud-service", () =>
    {
        // Fraud is degraded — payments still work without it (fail-open)
        return HealthCheckResult.Healthy("Fraud service is optional");
    }, tags: ["degraded"]);

// ─── App ─────────────────────────────────────────────────────────────────────
var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

// Auto-migrate on startup
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
}

app.MapControllers();

// Liveness — is the process alive?
app.MapHealthChecks("/health/live", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = _ => false // always returns healthy if process is running
});

// Readiness — are critical deps available?
app.MapHealthChecks("/health/ready", new Microsoft.AspNetCore.Diagnostics.HealthChecks.HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("critical")
});

// Full health — everything
app.MapHealthChecks("/health");

app.Run();
