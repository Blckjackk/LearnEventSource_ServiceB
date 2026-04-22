namespace CustomerProjectionService.Models;

public class CustomerCreatedEvent
{
    public Guid EventId { get; set; }
    public Guid AggregateId { get; set; }
    public DateTime OccurredAtUtc { get; set; }
    public int SchemaVersion { get; set; } = 1;
    public string EventType { get; set; } = "CustomerCreatedV1";
    public CustomerEventData Data { get; set; } = new();
}

public class CustomerUpdatedEvent
{
    public Guid EventId { get; set; }
    public Guid AggregateId { get; set; }
    public DateTime OccurredAtUtc { get; set; }
    public int SchemaVersion { get; set; } = 1;
    public string EventType { get; set; } = "CustomerUpdatedV1";
    public CustomerEventData Data { get; set; } = new();
}

public class CustomerDeletedEvent
{
    public Guid EventId { get; set; }
    public Guid AggregateId { get; set; }
    public DateTime OccurredAtUtc { get; set; }
    public int SchemaVersion { get; set; } = 1;
    public string EventType { get; set; } = "CustomerDeletedV1";
}

public class CustomerEventData
{
    public string Name { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public string Address { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
}
