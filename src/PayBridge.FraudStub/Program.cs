using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using PayBridge.FraudStub.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

var otelEndpoint = builder.Configuration["Otel:Endpoint"] ?? "http://otel-collector:4317";
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("PayBridge.FraudStub"))
    .WithTracing(t => t
        .AddAspNetCoreInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri(otelEndpoint)));

builder.Services.AddHealthChecks();

var app = builder.Build();
app.MapGrpcService<FraudDetectionService>();
app.MapHealthChecks("/health");
app.MapGet("/", () => "Fraud Detection gRPC Stub");
app.Run();
