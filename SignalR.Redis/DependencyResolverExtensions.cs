using SignalR.Infrastructure;
using SignalR.MessageBus;

namespace SignalR.Redis
{
    public static class DependencyResolverExtensions
    {
        public static IDependencyResolver UseRedis(IDependencyResolver resolver, string server, int port, string password, string eventKey)
        {
            return UseRedis(resolver, server, port, password, db: 0, eventKey: eventKey);
        }

        public static IDependencyResolver UseRedis(IDependencyResolver resolver, string server, int port, string password, int db, string eventKey)
        {
            resolver.Register(typeof(IMessageBus), () => new RedisMessageBus(server, port, password, db, eventKey, resolver));

            return resolver;
        }
    }
}
