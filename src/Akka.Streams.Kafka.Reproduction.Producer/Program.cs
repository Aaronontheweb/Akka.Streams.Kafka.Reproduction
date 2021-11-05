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
using Akka.Util.Internal;
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
            
            var kafkaHost = Environment.GetEnvironmentVariable("KAFKA_HOST") ?? "localhost";
            var kafkaPort = int.Parse(Environment.GetEnvironmentVariable("KAFKA_PORT") ?? "29092");

            var kafkaUserSasl = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
            var kafkaUserPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");

            var hasSasl = !(string.IsNullOrEmpty(kafkaUserSasl) || string.IsNullOrEmpty(kafkaUserPassword));
            
            var producerConfig = new ProducerConfig()
            {
            };

            if (hasSasl)
            {
                producerConfig.SaslMechanism = SaslMechanism.Plain;
                producerConfig.SaslUsername = kafkaUserSasl;
                producerConfig.SaslPassword = kafkaUserPassword;
            }

            var producerSettings = ProducerSettings<Null, string>.Create(actorSystem,
                    null, null)
                .WithBootstrapServers($"{kafkaHost}:{kafkaPort}");
            
            producerConfig.ForEach(kv => producerSettings = producerSettings.WithProperty(kv.Key, kv.Value));
            
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