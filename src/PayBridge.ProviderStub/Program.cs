using System.Text;
using System.Text.Json;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHttpClient();
builder.Services.AddHealthChecks();

var otelEndpoint = builder.Configuration["Otel:Endpoint"] ?? "http://otel-collector:4317";
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("PayBridge.ProviderStub"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddHttpClientInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri(otelEndpoint)));

var app = builder.Build();
var logger = app.Logger;
var rng    = new Random();

app.MapPost("/payments/submit", async (
    ProviderPaymentRequest request,
    IHttpClientFactory httpFactory,
    IConfiguration config) =>
{
    // Simulate provider processing delay
    await Task.Delay(rng.Next(100, 600));

    var providerTxId = $"prov_{Guid.NewGuid():N[..12]}";
    var isSuccess    = rng.NextDouble() < 0.92; // 92% success rate

    logger.LogInformation(
        "Provider processing payment {PaymentId} → {Result} txId={TxId}",
        request.payment_id, isSuccess ? "SUCCESS" : "FAILURE", providerTxId);

    // Fire webhook callback asynchronously (don't block the response)
    _ = Task.Run(async () =>
    {
        await Task.Delay(rng.Next(500, 2000)); // provider processes async

        var webhookUrl  = config["WebhookReceiver:Url"] ?? "http://webhook-receiver:8083";
        var callbackUrl = $"{webhookUrl}/webhooks/provider";

        var callback = new
        {
            providerTransactionId = providerTxId,
            status    = isSuccess ? "SUCCESS" : "FAILURE",
            timestamp = DateTime.UtcNow,
            metadata  = new Dictionary<string, string>
            {
                ["paybridge_payment_id"] = request.payment_id
            }
        };

        try
        {
            var http = httpFactory.CreateClient();
            var json = JsonSerializer.Serialize(callback);
            var content = new StringContent(json, Encoding.UTF8, "application/json");
            var resp = await http.PostAsync(callbackUrl, content);
            logger.LogInformation("Webhook sent for {PaymentId} → HTTP {Status}", request.payment_id, resp.StatusCode);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to send webhook for payment {PaymentId}", request.payment_id);
        }
    });

    return Results.Ok(new { transactionId = providerTxId, status = "processing" });
});

app.MapHealthChecks("/health");
app.MapGet("/", () => "Payment Provider Stub");
app.Run();

record ProviderPaymentRequest(
    string payment_id,
    string merchant_id,
    decimal amount,
    string currency,
    string method
);
