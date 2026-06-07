using Microsoft.EntityFrameworkCore;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OpenTelemetry.Metrics;
using PayBridge.SettlementConsumer.Infrastructure;
using PayBridge.SettlementConsumer.Workers;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();
builder.Host.UseSerilog();

builder.Services.AddDbContext<AppDbContext>(opt =>
    opt.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

var otelEndpoint = builder.Configuration["Otel:Endpoint"] ?? "http://otel-collector:4317";
builder.Services.AddOpenTelemetry()
    .ConfigureResource(r => r.AddService("PayBridge.SettlementConsumer"))
    .WithTracing(t => t
        .AddSource("PayBridge.SettlementConsumer")
        .AddEntityFrameworkCoreInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri(otelEndpoint)))
    .WithMetrics(m => m
        .AddMeter("PayBridge.SettlementConsumer")
        .AddRuntimeInstrumentation()
        .AddOtlpExporter(o => o.Endpoint = new Uri(otelEndpoint)));

builder.Services.AddHostedService<SettlementWorker>();
builder.Services.AddHealthChecks()
    .AddNpgSql(builder.Configuration.GetConnectionString("Postgres")!, name: "postgresql");

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    await db.Database.EnsureCreatedAsync();
}

app.MapHealthChecks("/health");
app.Run();
