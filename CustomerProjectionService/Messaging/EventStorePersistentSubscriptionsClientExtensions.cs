using EventStore.Client;

namespace CustomerProjectionService.Messaging;

public static class EventStorePersistentSubscriptionsClientExtensions
{
    public static Task<PersistentSubscription> SubscribeToStreamAsync(
        this EventStorePersistentSubscriptionsClient client,
        string streamName,
        string groupName,
        Func<PersistentSubscription, ResolvedEvent, int?, CancellationToken, Task> eventAppeared,
        Action<PersistentSubscription, SubscriptionDroppedReason, Exception>? subscriptionDropped = null,
        UserCredentials? userCredentials = null,
        int bufferSize = 10,
        bool autoAck = false,
        CancellationToken cancellationToken = default)
    {
        return client.SubscribeAsync(
            streamName,
            groupName,
            eventAppeared,
            subscriptionDropped,
            userCredentials,
            bufferSize,
            autoAck,
            cancellationToken);
    }
}
