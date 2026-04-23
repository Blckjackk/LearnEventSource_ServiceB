using System.Text;
using System.Text.Json;
using CustomerProjectionService.Data;
using CustomerProjectionService.Models;
using EventStore.Client;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace CustomerProjectionService.Messaging;

public class EventStorePersistentSubscriptionWorker : BackgroundService
{
    private readonly EventStorePersistentSubscriptionsClient _client;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<EventStorePersistentSubscriptionWorker> _logger;

    public EventStorePersistentSubscriptionWorker(
        EventStorePersistentSubscriptionsClient client,
        IServiceScopeFactory scopeFactory,
        IConfiguration configuration,
        ILogger<EventStorePersistentSubscriptionWorker> logger)
    {
        _client = client;
        _scopeFactory = scopeFactory;
        _configuration = configuration;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var streamName = _configuration["EventStore:Subscription:Stream"] ?? "CustomerStream";
        var groupName = _configuration["EventStore:Subscription:Group"] ?? "service-b-sql-group";

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _logger.LogInformation("Connecting persistent subscription Stream={Stream} Group={Group}", streamName, groupName);

                await _client.SubscribeAsync(
                    streamName,
                    groupName,
                    EventAppeared,
                    SubscriptionDropped,
                    cancellationToken: stoppingToken);

                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Persistent subscription disconnected. Reconnecting in 5 seconds.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private void SubscriptionDropped(PersistentSubscription subscription, SubscriptionDroppedReason reason, Exception? ex)
    {
        _logger.LogWarning(ex, "Subscription dropped: {Reason}", reason);
    }

    private async Task EventAppeared(PersistentSubscription subscription, ResolvedEvent evnt, int? retryCount, CancellationToken cancellationToken)
    {
        try
        {
            var eventType = evnt.Event.EventType;
            if (string.IsNullOrWhiteSpace(eventType) || eventType.StartsWith("$", StringComparison.Ordinal))
            {
                await subscription.Ack(evnt);
                return;
            }

            var json = Encoding.UTF8.GetString(evnt.Event.Data.ToArray());

            Guid eventId;
            Guid aggregateId;
            CustomerEventData? data = null;

            if (eventType == "CustomerCreatedV1")
            {
                var evt = JsonSerializer.Deserialize<CustomerCreatedEvent>(json);
                if (evt is null)
                {
                    await subscription.Nack(PersistentSubscriptionNakEventAction.Park, "Invalid create payload", evnt);
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
                data = evt.Data;
            }
            else if (eventType == "CustomerUpdatedV1")
            {
                var evt = JsonSerializer.Deserialize<CustomerUpdatedEvent>(json);
                if (evt is null)
                {
                    await subscription.Nack(PersistentSubscriptionNakEventAction.Park, "Invalid update payload", evnt);
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
                data = evt.Data;
            }
            else if (eventType == "CustomerDeletedV1")
            {
                var evt = JsonSerializer.Deserialize<CustomerDeletedEvent>(json);
                if (evt is null)
                {
                    await subscription.Nack(PersistentSubscriptionNakEventAction.Park, "Invalid delete payload", evnt);
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
            }
            else
            {
                _logger.LogInformation("Unknown event type {EventType}. Ack and skip.", eventType);
                await subscription.Ack(evnt);
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var alreadyProcessed = await db.ProcessedEvents.AnyAsync(x => x.EventId == eventId, cancellationToken);
            if (alreadyProcessed)
            {
                await subscription.Ack(evnt);
                return;
            }

            var existing = await db.Customers.FirstOrDefaultAsync(x => x.Id == aggregateId, cancellationToken);
            if (eventType == "CustomerDeletedV1")
            {
                if (existing is not null)
                {
                    existing.IsDeleted = true;
                }
            }
            else if (existing is null)
            {
                db.Customers.Add(new Customer
                {
                    Id = aggregateId,
                    Name = data?.Name ?? string.Empty,
                    IsDeleted = false
                });
            }
            else
            {
                existing.Name = data?.Name ?? string.Empty;
                existing.IsDeleted = false;
            }

            db.ProcessedEvents.Add(new ProcessedEvent
            {
                EventId = eventId,
                ProcessedAtUtc = DateTime.UtcNow
            });

            await db.SaveChangesAsync(cancellationToken);
            await subscription.Ack(evnt);
        }
        catch (DbUpdateException ex) when (IsDuplicateKey(ex))
        {
            await subscription.Ack(evnt);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process event. RetryCount={RetryCount}", retryCount ?? 0);
            var action = (retryCount ?? 0) >= 5
                ? PersistentSubscriptionNakEventAction.Park
                : PersistentSubscriptionNakEventAction.Retry;
            await subscription.Nack(action, ex.Message, evnt);
        }
    }

    private static bool IsDuplicateKey(DbUpdateException ex)
    {
        if (ex.InnerException is not SqlException sqlEx)
        {
            return false;
        }

        return sqlEx.Number is 2601 or 2627;
    }
}