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
    private const string CheckpointId = "customer-projection";

    private readonly EventStoreClient _client;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<EventStorePersistentSubscriptionWorker> _logger;

    public EventStorePersistentSubscriptionWorker(
        EventStoreClient client,
        IServiceScopeFactory scopeFactory,
        ILogger<EventStorePersistentSubscriptionWorker> logger)
    {
        _client = client;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var position = await LoadCheckpointAsync(stoppingToken);
        _logger.LogInformation(
            "EventStore projection worker starting from checkpoint Commit={CommitPosition}, Prepare={PreparePosition}",
            position.CommitPosition,
            position.PreparePosition);

        var retryCount = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                retryCount = 0;
                var readFromPosition = position;
                _logger.LogDebug("Starting catch-up from position: Commit={CommitPosition}, Prepare={PreparePosition}", 
                    readFromPosition.CommitPosition, readFromPosition.PreparePosition);
                
                var hasProgress = false;
                var page = _client.ReadAllAsync(
                    Direction.Forwards,
                    readFromPosition,
                    maxCount: 100,
                    cancellationToken: stoppingToken);

                await foreach (var resolvedEvent in page)
                {
                    var originalPosition = resolvedEvent.OriginalPosition.GetValueOrDefault();

                    // ReadAllAsync reads from an inclusive position. Skip the anchor event at checkpoint
                    // so we don't replay the same last event forever.
                    if (originalPosition.CommitPosition == readFromPosition.CommitPosition
                        && originalPosition.PreparePosition == readFromPosition.PreparePosition)
                    {
                        continue;
                    }

                    hasProgress = true;
                    await ProcessEvent(resolvedEvent, stoppingToken);

                    // Persist the exact last processed position.
                    var lastProcessedPosition = new Position(originalPosition.CommitPosition, originalPosition.PreparePosition);
                    await SaveCheckpointAsync(lastProcessedPosition, stoppingToken);
                    position = lastProcessedPosition;
                }

                if (!hasProgress)
                {
                    _logger.LogDebug("No more events, waiting 1 second before polling again");
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("EventStore projection worker stopping");
                break;
            }
            catch (Exception ex)
            {
                if (ex is InvalidOperationException && ex.Message.Contains("InvalidPosition", StringComparison.OrdinalIgnoreCase))
                {
                    _logger.LogWarning(ex, "Checkpoint position invalid. Resetting checkpoint to Position.Start.");
                    position = Position.Start;
                    await SaveCheckpointAsync(position, stoppingToken);
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    continue;
                }

                retryCount++;
                _logger.LogError(ex, "Error in catch-up subscription (retry {RetryCount}). Waiting 5 seconds.", retryCount);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }
    }

    private async Task<Position> LoadCheckpointAsync(CancellationToken cancellationToken)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

            var checkpoint = await db.ProjectionCheckpoints.AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == CheckpointId, cancellationToken);

            if (checkpoint is null)
            {
                return Position.Start;
            }

            return new Position(checkpoint.CommitPosition, checkpoint.PreparePosition);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load checkpoint. Falling back to Position.Start.");
            return Position.Start;
        }
    }

    private async Task SaveCheckpointAsync(Position position, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var checkpoint = await db.ProjectionCheckpoints
            .FirstOrDefaultAsync(x => x.Id == CheckpointId, cancellationToken);

        if (checkpoint is null)
        {
            checkpoint = new ProjectionCheckpoint { Id = CheckpointId };
            db.ProjectionCheckpoints.Add(checkpoint);
        }

        checkpoint.CommitPosition = position.CommitPosition;
        checkpoint.PreparePosition = position.PreparePosition;
        checkpoint.UpdatedAtUtc = DateTime.UtcNow;

        await db.SaveChangesAsync(cancellationToken);
    }

    private async Task ProcessEvent(ResolvedEvent evnt, CancellationToken cancellationToken)
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
        DateTime occurredAtUtc;
        CustomerEventData? data = null;

        if (eventType == "CustomerCreatedV1")
            {
                var evt = JsonSerializer.Deserialize<CustomerCreatedEvent>(json);
                if (evt is null)
                {
                    _logger.LogWarning("Failed to deserialize CustomerCreatedV1 event. Skipping event.");
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
                occurredAtUtc = evt.OccurredAtUtc;
                data = evt.Data;
            }
            else if (eventType == "CustomerUpdatedV1")
            {
                var evt = JsonSerializer.Deserialize<CustomerUpdatedEvent>(json);
                if (evt is null)
                {
                    _logger.LogWarning("Failed to deserialize CustomerUpdatedV1 event. Skipping event.");
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
                occurredAtUtc = evt.OccurredAtUtc;
                data = evt.Data;
            }
            else if (eventType == "CustomerDeletedV1")
            {
                var evt = JsonSerializer.Deserialize<CustomerDeletedEvent>(json);
                if (evt is null)
                {
                    _logger.LogWarning("Failed to deserialize CustomerDeletedV1 event. Skipping event.");
                    return;
                }

                eventId = evt.EventId;
                aggregateId = evt.AggregateId;
                occurredAtUtc = evt.OccurredAtUtc;
            }
            else
            {
                _logger.LogInformation("Unknown event type {EventType}. Skipping event.", eventType);
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
                existing.UpdatedAtUtc = occurredAtUtc;
                _logger.LogInformation("Marked customer as deleted: AggregateId={AggregateId}", aggregateId);
            }
        }
        else if (existing is null)
        {
            db.Customers.Add(new Customer
            {
                Id = aggregateId,
                Name = data?.Name ?? string.Empty,
                Phone = data?.Phone ?? string.Empty,
                Address = data?.Address ?? string.Empty,
                City = data?.City ?? string.Empty,
                Country = data?.Country ?? string.Empty,
                Email = data?.Email ?? string.Empty,
                IsDeleted = false,
                UpdatedAtUtc = occurredAtUtc
            });
            _logger.LogInformation("Created new customer: AggregateId={AggregateId}, Name={Name}", aggregateId, data?.Name);
        }
        else
        {
            existing.Name = data?.Name ?? string.Empty;
            existing.Phone = data?.Phone ?? string.Empty;
            existing.Address = data?.Address ?? string.Empty;
            existing.City = data?.City ?? string.Empty;
            existing.Country = data?.Country ?? string.Empty;
            existing.Email = data?.Email ?? string.Empty;
            existing.IsDeleted = false;
            existing.UpdatedAtUtc = occurredAtUtc;
            _logger.LogInformation("Updated customer: AggregateId={AggregateId}, Name={Name}", aggregateId, data?.Name);
        }

        db.ProcessedEvents.Add(new ProcessedEvent
        {
            EventId = eventId,
            ProcessedAtUtc = DateTime.UtcNow
        });

        try
        {
            await db.SaveChangesAsync(cancellationToken);
            _logger.LogInformation("Successfully processed event: EventId={EventId}, AggregateId={AggregateId}", eventId, aggregateId);
        }
        catch (DbUpdateException ex) when (IsDuplicateKey(ex))
        {
            // Safe to continue: another concurrent path may have inserted the same event id.
            _logger.LogInformation("Duplicate key exception while saving EventId={EventId}. Skipping as already processed.", eventId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to persist event {EventId} from stream {Stream}. Will retry catch-up.", eventId, evnt.Event.EventStreamId);
            throw;
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