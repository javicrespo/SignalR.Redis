using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using BookSleeve;
using ProtoBuf;
using SignalR.Infrastructure;
using SignalR.MessageBus;

namespace SignalR.Redis
{
    public class RedisMessageBus : IMessageBus, IIdGenerator<long>
    {
        private readonly InProcessMessageBus<long> _bus;
        private readonly int _db;
        private readonly string _eventKey;
        private readonly string _server;
        private readonly int _port;
        private readonly string _password;

        private RedisConnection _connection;
        private bool _connectionReady;
        private Task _connectingTask;

        public RedisMessageBus(string server, int port, string password, int db, string eventKey, IDependencyResolver resolver)
        {
            _server = server;
            _port = port;
            _password = password;
            _db = db;
            _eventKey = eventKey;
            _bus = new InProcessMessageBus<long>(resolver, this);

            EnsureConnection();
        }

        private bool ConnectionReady
        {
            get
            {
                return  _connectionReady && 
                        _connection != null && 
                        _connection.State == RedisConnectionBase.ConnectionState.Open;
            }
        }

        public Task<MessageResult> GetMessages(IEnumerable<string> eventKeys, string id, CancellationToken timeoutToken)
        {
            return _bus.GetMessages(eventKeys, id, timeoutToken);
        }

        public Task Send(string connectionId, string eventKey, object value)
        {
            var message = new Message(connectionId, eventKey, value);
            if (ConnectionReady)
            {
                // Publish to all nodes
                return _connection.Publish(_eventKey, message.GetBytes());
            }

            // Open the connection if it isn't ready then publish to all nodes
            return OpenConnection().Then((conn, m) => conn.Publish(_eventKey, m.GetBytes()), _connection, message);
        }

        private Task OpenConnection()
        {
            if (ConnectionReady)
            {
                // Nothing to do here
                return TaskAsyncHelper.Empty;
            }

            try
            {
                EnsureConnection();
                return _connectingTask.Catch();
            }
            catch (Exception ex)
            {
                return TaskAsyncHelper.FromError(ex);
            }
        }

        private void EnsureConnection()
        {
            var tcs = new TaskCompletionSource<object>();

            if (Interlocked.CompareExchange(ref _connectingTask, tcs.Task, null) != null)
            {
                // Give all clients the same task for reconnecting
                return;
            }

            try
            {
                if (_connection == null)
                {
                    _connection = new RedisConnection(_server, _port, password: _password);
                    _connection.Closed += (sender, e) =>
                    {
                        // Reconnect on close
                        Disconnect();
                        EnsureConnection();
                    };
                }

                // Open the connection
                _connection.Open().ContinueWith(task =>
                {
                    if (task.IsFaulted)
                    {
                        tcs.SetException(task.Exception);
                    }
                    else if (task.IsCanceled)
                    {
                        tcs.SetCanceled();
                    }
                    else
                    {
                        try
                        {
                            // Initialize the subscriber channel
                            var channel = _connection.GetOpenSubscriberChannel();

                            // Subscribe to the bus event
                            channel.Subscribe(_eventKey, OnMessage);

                            // Mark the connection as ready
                            _connectionReady = true;

                            // Mark the task as completed
                            tcs.SetResult(null);
                        }
                        catch (Exception ex)
                        {
                            tcs.SetException(ex);
                        }
                    }
                });
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
        }

        private void Disconnect()
        {
            Interlocked.Exchange(ref _connectingTask, null);
            _connectionReady = false;
            _connection = null;
        }

        private void OnMessage(string key, byte[] data)
        {
            var message = Message.Deserialize(data);

            try
            {
                // Save it to the local buffer
                _bus.Send(message.ConnectionId, message.EventKey, message.Value).Catch();
            }
            catch (Exception ex)
            {
                // TODO: Handle this better
                Debug.WriteLine(ex.Message);
            }
        }

        public long ConvertFromString(string value)
        {
            return Int64.Parse(value, CultureInfo.InvariantCulture);
        }

        public string ConvertToString(long value)
        {
            return value.ToString(CultureInfo.InvariantCulture);
        }

        // TODO: Consider implementing something like flake (http://blog.boundary.com/2012/01/12/Flake-A-decentralized-k-ordered-id-generation-service-in-Erlang.html)
        public long GetNext()
        {            
            // TODO: Make this non-blocking in SignalR
            return _connection.Wait(_connection.Strings.Increment(_db, _eventKey));
        }

        [ProtoContract]
        private class Message
        {
            public Message()
            {
            }

            public Message(string connectionId, string eventKey, object value)
            {
                ConnectionId = connectionId;
                EventKey = eventKey;
                Value = value.ToString();
            }

            [ProtoMember(1)]
            public string ConnectionId { get; set; }

            [ProtoMember(2)]
            public string EventKey { get; set; }

            [ProtoMember(3)]
            public string Value { get; set; }

            public byte[] GetBytes()
            {
                using (var ms = new MemoryStream())
                {
                    Serializer.Serialize(ms, this);
                    return ms.ToArray();
                }
            }

            public static Message Deserialize(byte[] data)
            {
                using (var ms = new MemoryStream(data))
                {
                    return Serializer.Deserialize<Message>(ms);
                }
            }
        }
    }
}
