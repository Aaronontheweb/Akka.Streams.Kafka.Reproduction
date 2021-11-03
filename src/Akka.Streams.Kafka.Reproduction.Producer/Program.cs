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

namespace Akka.Streams.Kafka.Reproduction.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configSetup = BootstrapSetup.Create().WithConfig(KafkaExtensions.DefaultSettings);
            var actorSystem = ActorSystem.Create("KafkaSpec", configSetup);
            var materializer = actorSystem.Materializer();

            var producerSettings = ProducerSettings<Null, string>.Create(actorSystem,
                    null, null)
                .WithBootstrapServers("localhost:29092");
            
            BeginProduce(producerSettings, materializer);

            await actorSystem.WhenTerminated;
        }
        
        private static async Task BeginProduce(ProducerSettings<Null, string> producerSettings, ActorMaterializer materializer)
        {
            await Source
                .Cycle(() => Enumerable.Range(1, 100).GetEnumerator())
                .Select(c => c.ToString())
                .Select(elem => ProducerMessage.Single(new ProducerRecord<Null, string>("akka-input", elem)))
                .BackpressureAlert(LogLevel.WarningLevel, TimeSpan.FromMilliseconds(500))
                .WithAttributes(Attributes.CreateName("FlexiFlow-outbound"))
                .Via(KafkaProducer.FlexiFlow<Null, string, NotUsed>(producerSettings))
                .Select(result =>
                {
                    var response = (Result<Null, string, NotUsed>) result;
                    Console.WriteLine(
                        $"Producer: {response.Metadata.Topic}/{response.Metadata.Partition} {response.Metadata.Offset}: {response.Metadata.Value}");
                    return result;
                })
                .RunWith(Sink.Ignore<IResults<Null, string, NotUsed>>(), materializer).ConfigureAwait(false);
        }
    }
}