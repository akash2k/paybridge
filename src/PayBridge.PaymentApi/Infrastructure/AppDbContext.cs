using Microsoft.EntityFrameworkCore;
using PayBridge.Shared.Models;

namespace PayBridge.PaymentApi.Infrastructure;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<Payment> Payments => Set<Payment>();
    public DbSet<OutboxEvent> OutboxEvents => Set<OutboxEvent>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Payment>(e =>
        {
            e.HasKey(p => p.Id);
            e.HasIndex(p => new { p.MerchantId, p.IdempotencyKey }).IsUnique();
            e.Property(p => p.Amount).HasPrecision(18, 4);
            e.Property(p => p.Status).HasConversion<string>();
            e.Property(p => p.Method).HasConversion<string>();
        });

        modelBuilder.Entity<OutboxEvent>(e =>
        {
            e.HasKey(o => o.Id);
            e.Property(o => o.Id).UseIdentityColumn();
            e.HasIndex(o => o.ProcessedAt);
        });
    }
}
