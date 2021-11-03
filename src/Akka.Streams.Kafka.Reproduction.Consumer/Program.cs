using System;
using System.Linq;
using System.Threading.Tasks;
using Aaron.Akka.Streams.Dsl;
using Akka.Actor;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Reproduction
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configSetup = BootstrapSetup.Create().WithConfig(KafkaExtensions.DefaultSettings);
            var actorSystem = ActorSystem.Create("KafkaSpec", configSetup);
            var materializer = actorSystem.Materializer();

            var consumerConfig = new ConsumerConfig
            {
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = true,
                AutoOffsetReset = AutoOffsetReset.Latest,
                ClientId = "unique.client",
                SocketKeepaliveEnable = true,
                ConnectionsMaxIdleMs = 180000
            };
            
        }
        
        
    }
}
