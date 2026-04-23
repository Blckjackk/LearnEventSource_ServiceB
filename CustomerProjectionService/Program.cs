using CustomerProjectionService.Messaging;
using CustomerProjectionService.Data;
using Microsoft.EntityFrameworkCore;
using EventStore.Client;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseSqlServer(
        builder.Configuration.GetConnectionString("DefaultConnection"),
        sqlOptions => sqlOptions.EnableRetryOnFailure()));

builder.Services.AddSingleton(sp =>
{
    var conn = builder.Configuration["EventStore:ConnectionString"]
               ?? "esdb://admin:changeit@localhost:2113?tls=false";
    return new EventStoreClient(EventStoreClientSettings.Create(conn));
});
builder.Services.AddHostedService<EventStorePersistentSubscriptionWorker>();

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    db.Database.Migrate();
}

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
