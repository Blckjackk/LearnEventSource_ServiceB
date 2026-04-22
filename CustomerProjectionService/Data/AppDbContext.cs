using CustomerProjectionService.Models;
using Microsoft.EntityFrameworkCore;

namespace CustomerProjectionService.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {

    }

    public DbSet<Customer> Customers { get; set; }
    public DbSet<ProcessedEvent> ProcessedEvents { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ProcessedEvent>().HasKey(x => x.EventId);
        base.OnModelCreating(modelBuilder);
    }
}
