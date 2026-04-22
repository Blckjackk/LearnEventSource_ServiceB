using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;
using CustomerProjectionService.Models;
using CustomerProjectionService.Data;
using Microsoft.Data.SqlClient;

namespace CustomerProjectionService.Messaging;

public class RabbitMqConsumer
{
    private const string ExchangeName = "customer_events";
    private const string QueueName = "customer_created.service_b";

    private readonly IServiceProvider _serviceProvider;
    public RabbitMqConsumer(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
    }

    public void Start()
    {
        try
        {
            Console.WriteLine("Service B lagi mau ngedengerin Subs di Rabbit ini");
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };

            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();

            Console.WriteLine("Service B sudah selesai ngebuat factory, connection, dan channel");

            channel.ExchangeDeclare(
                exchange: ExchangeName,
                type: ExchangeType.Fanout,
                durable: false,
                autoDelete: false,
                arguments: null
            );
            channel.QueueDeclare(
                queue: QueueName,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null
            );

            channel.QueueBind(QueueName, ExchangeName, routingKey: "");
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            Console.WriteLine($"✓ Queue declared and bound: {QueueName}");

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                try
                {
                    Console.WriteLine("\n--- Consumer received message ---");
                    var body = ea.Body.ToArray();
                    var json = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"JSON: {json}"); 

                    using var jsonDoc = JsonDocument.Parse(json);
                    if (!jsonDoc.RootElement.TryGetProperty("EventType", out var eventTypeElement))
                    {
                        Console.WriteLine("❌ EventType missing. Dropping message.");
                        channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                        return;
                    }

                    var eventType = eventTypeElement.GetString() ?? string.Empty;

                    Guid eventId;
                    Guid aggregateId;
                    DateTime occurredAtUtc;
                    CustomerEventData? data = null;

                    if (eventType == "CustomerCreatedV1")
                    {
                        var evt = JsonSerializer.Deserialize<CustomerCreatedEvent>(json);
                        if (evt is null)
                        {
                            Console.WriteLine("❌ Create event deserialization failed. Dropping message.");
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
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
                            Console.WriteLine("❌ Update event deserialization failed. Dropping message.");
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
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
                            Console.WriteLine("❌ Delete event deserialization failed. Dropping message.");
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                            return;
                        }

                        eventId = evt.EventId;
                        aggregateId = evt.AggregateId;
                        occurredAtUtc = evt.OccurredAtUtc;
                    }
                    else
                    {
                        Console.WriteLine($"⚠ Unknown event type {eventType}. Ack and skip.");
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        return;
                    }

                    Console.WriteLine($"✓ Event: {eventType} EventId={eventId} AggregateId={aggregateId}");

                    using var scope = _serviceProvider.CreateScope();
                    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                    Console.WriteLine("✓ DbContext acquired");

                    var alreadyProcessed = db.ProcessedEvents.Any(x => x.EventId == eventId);
                    if (alreadyProcessed)
                    {
                        Console.WriteLine("↩ Event already processed. Skip.");
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        return;
                    }

                    var existing = db.Customers.FirstOrDefault(x => x.Id == aggregateId);
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
                            IsDeleted = false,
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

                    db.SaveChanges();
                    Console.WriteLine("✓✓✓ DATA TERSIMPAN KE DATABASE B ✓✓✓");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"❌ ERROR in Consumer.Received: {ex.Message}");
                    Console.WriteLine($"❌ Stack Trace: {ex.StackTrace}");

                    var requeue = ShouldRequeue(ex);
                    channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: requeue);
                }
            };

            channel.BasicConsume(
                queue: QueueName,
                autoAck: false,
                consumer: consumer
            );
            Console.WriteLine("✓ Consumer listening...");
            Console.WriteLine("=== CONSUMER READY ===\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ ERROR in Start(): {ex.Message}");
            Console.WriteLine($"❌ Stack Trace: {ex.StackTrace}");
        }
    }

    private static bool ShouldRequeue(Exception ex)
    {
        if (ex is ObjectDisposedException)
        {
            return false;
        }

        if (ex is InvalidOperationException && ex.Message.Contains("disposed object", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var sqlEx = ex as SqlException ?? ex.InnerException as SqlException;
        if (sqlEx is not null)
        {
            // Database unavailable/login failed are not recoverable by immediate requeue.
            if (sqlEx.Number is 4060 or 18456)
            {
                return false;
            }
        }

        // Keep retry for likely transient failures.
        return true;
    }
}
