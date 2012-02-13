using System;
using SignalR.Infrastructure;
using SignalR.MessageBus;

namespace SignalR.Redis
{
    public static class DependencyResolverExtensions
    {
        public static IDependencyResolver UseRedis(this IDependencyResolver resolver, string server, int port, string password, string eventKey)
        {
            return UseRedis(resolver, server, port, password, db: 0, eventKey: eventKey);
        }

        public static IDependencyResolver UseRedis(this IDependencyResolver resolver, string server, int port, string password, int db, string eventKey)
        {
            var bus = new Lazy<RedisMessageBus>(() => new RedisMessageBus(server, port, password, db, eventKey, resolver));
            resolver.Register(typeof(IMessageBus), () => bus.Value);

            return resolver;
        }
    }
}
