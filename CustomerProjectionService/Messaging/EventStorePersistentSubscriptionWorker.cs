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
    private readonly EventStoreClient _client;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IConfiguration _configuration;
    private readonly ILogger<EventStorePersistentSubscriptionWorker> _logger;

    public EventStorePersistentSubscriptionWorker(
        EventStoreClient client,
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
        _logger.LogInformation("EventStore projection worker starting (SubscribeToAll + idempotent DB writes)");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _client.SubscribeToAllAsync(
                    Position.Start,
                    EventAppeared,
                    subscriptionDropped: SubscriptionDropped,
                    cancellationToken: stoppingToken);

                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("EventStore projection worker stopping");
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Subscription disconnected/failed. Reconnecting in 5 seconds.");
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private void SubscriptionDropped(StreamSubscription subscription, SubscriptionDroppedReason reason, Exception? ex)
    {
        _logger.LogWarning(ex, "SubscribeToAll dropped: {Reason}", reason);
    }

    private async Task EventAppeared(StreamSubscription subscription, ResolvedEvent evnt, CancellationToken cancellationToken)
    {
        try
        {
            if (evnt.Event.EventStreamId.StartsWith("$", StringComparison.Ordinal))
            {
                return;
            }

            if (!evnt.Event.EventStreamId.StartsWith("customer-", StringComparison.Ordinal))
            {
                return;
            }

            var eventType = evnt.Event.EventType;
            _logger.LogInformation("Event received: Type={EventType}, Stream={Stream}, EventNumber={EventNumber}",
                eventType, evnt.Event.EventStreamId, evnt.Event.EventNumber);
            
            if (string.IsNullOrWhiteSpace(eventType) || eventType.StartsWith("$", StringComparison.Ordinal))
            {
                _logger.LogDebug("Skipping system event: {EventType}", eventType);
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
                    _logger.LogWarning("Failed to deserialize CustomerCreatedV1 event");
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
                    _logger.LogWarning("Failed to deserialize CustomerUpdatedV1 event");
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
                    _logger.LogWarning("Failed to deserialize CustomerDeletedV1 event");
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
            }
            else
            {
                _logger.LogInformation("Unknown event type {EventType}. Ack and skip.", eventType);
                return;
            }

            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var alreadyProcessed = await db.ProcessedEvents.AnyAsync(x => x.EventId == eventId, cancellationToken);
            if (alreadyProcessed)
            {
                _logger.LogDebug("Event already processed: EventId={EventId}", eventId);
                return;
            }

            var existing = await db.Customers.FirstOrDefaultAsync(x => x.Id == aggregateId, cancellationToken);
            if (eventType == "CustomerDeletedV1")
            {
                if (existing is not null)
                {
                    existing.IsDeleted = true;
                    _logger.LogInformation("Marked customer as deleted: AggregateId={AggregateId}", aggregateId);
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
                _logger.LogInformation("Created new customer: AggregateId={AggregateId}, Name={Name}", aggregateId, data?.Name);
            }
            else
            {
                existing.Name = data?.Name ?? string.Empty;
                existing.IsDeleted = false;
                _logger.LogInformation("Updated customer: AggregateId={AggregateId}, Name={Name}", aggregateId, data?.Name);
            }

            db.ProcessedEvents.Add(new ProcessedEvent
            {
                EventId = eventId,
                ProcessedAtUtc = DateTime.UtcNow
            });

            await db.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Successfully processed event: EventId={EventId}, AggregateId={AggregateId}", eventId, aggregateId);
        }
        catch (DbUpdateException ex) when (IsDuplicateKey(ex))
        {
            _logger.LogInformation("Duplicate key exception, still acknowledging event to move forward");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process event from stream {Stream}", evnt.Event.EventStreamId);
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