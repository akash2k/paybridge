using Microsoft.EntityFrameworkCore;
using PayBridge.Shared.Models;

namespace PayBridge.SettlementConsumer.Infrastructure;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<SettlementRecord> SettlementRecords => Set<SettlementRecord>();
    public DbSet<Shared.Models.Payment> Payments => Set<Shared.Models.Payment>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SettlementRecord>(e =>
        {
            e.HasKey(s => s.Id);
            e.Property(s => s.Id).UseIdentityColumn();
            e.HasIndex(s => s.PaymentId).IsUnique();
            e.Property(s => s.Amount).HasPrecision(18, 4);
            e.Property(s => s.FinalStatus).HasConversion<string>();
        });

        modelBuilder.Entity<Shared.Models.Payment>(e =>
        {
            e.HasKey(p => p.Id);
            e.Property(p => p.Amount).HasPrecision(18, 4);
            e.Property(p => p.Status).HasConversion<string>();
            e.Property(p => p.Method).HasConversion<string>();
        });
    }
}
