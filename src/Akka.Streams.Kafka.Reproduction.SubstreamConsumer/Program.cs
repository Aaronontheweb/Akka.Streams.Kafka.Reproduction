using System;
using System.Linq;
using System.Threading.Tasks;
using Aaron.Akka.Streams.Dsl;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Streams.Dsl;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Helpers;
using Akka.Streams.Kafka.Messages;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;

namespace Akka.Streams.Kafka.Reproduction.SubstreamConsumer
{
    class Program
    {
         static async Task Main(string[] args)
        {
            var configSetup = BootstrapSetup.Create().WithConfig(ConfigurationFactory.ParseString(@"akka.loglevel = DEBUG")
                .WithFallback(KafkaExtensions.DefaultSettings));
            var actorSystem = ActorSystem.Create("KafkaSpec", configSetup);
            var materializer = actorSystem.Materializer();

            var kafkaHost = Environment.GetEnvironmentVariable("KAFKA_HOST") ?? "localhost";
            var kafkaPort = int.Parse(Environment.GetEnvironmentVariable("KAFKA_PORT") ?? "29092");

            var kafkaUserSasl = Environment.GetEnvironmentVariable("KAFKA_SASL_USERNAME");
            var kafkaUserPassword = Environment.GetEnvironmentVariable("KAFKA_SASL_PASSWORD");

            var hasSasl = !(string.IsNullOrEmpty(kafkaUserSasl) || string.IsNullOrEmpty(kafkaUserPassword));
            
            var consumerConfig = new ConsumerConfig
            {
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                AllowAutoCreateTopics = true,
                AutoOffsetReset = AutoOffsetReset.Latest,
                ClientId = "substream.client",
                SocketKeepaliveEnable = true,
                ConnectionsMaxIdleMs = 180000,
            };

            var consumerSettings = ConsumerSettings<Null, string>
                .Create(actorSystem, null, null)
                .WithBootstrapServers($"{kafkaHost}:{kafkaPort}")
                .WithStopTimeout(TimeSpan.Zero)
                .WithGroupId("group1");
            
            var producerConfig = new ProducerConfig()
            {
            };

            if (hasSasl)
            {
                actorSystem.Log.Info("Using SASL...");
                consumerConfig.SaslMechanism = SaslMechanism.Plain;
                consumerConfig.SaslUsername = kafkaUserSasl;
                consumerConfig.SaslPassword = kafkaUserPassword;
                consumerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;

                producerConfig.SaslMechanism = SaslMechanism.Plain;
                producerConfig.SaslUsername = kafkaUserSasl;
                producerConfig.SaslPassword = kafkaUserPassword;
                producerConfig.SecurityProtocol = SecurityProtocol.SaslSsl;
            }
            
            var producerSettings = ProducerSettings<Null, string>.Create(actorSystem,
                    null, null)
                .WithBootstrapServers($"{kafkaHost}:{kafkaPort}");
            
            // TODO: we should just be able to accept a `ConsumerConfig` property
            consumerConfig.ForEach(kv => consumerSettings = consumerSettings.WithProperty(kv.Key, kv.Value));
            producerConfig.ForEach(kv => producerSettings = producerSettings.WithProperty(kv.Key, kv.Value));
            
            var committerSettings = CommitterSettings.Create(actorSystem);

            var mappingFlow = (Flow<CommittableMessage<Null, string>, (string Topic, string Value, ICommittableOffset CommitableOffset), NotUsed>)Flow.Create<CommittableMessage<Null, string>>()
                .GroupBy(10, message => message.Record.Partition.Value)
                .Select(c => (c.Record.Topic, c.Record.Message.Value, c.CommitableOffset))
                .SelectAsync(1, async t =>
                {
                    // simulate a request-response call that takes 10ms to complete here
                    await Task.Delay(10);
                    return t;
                })
                .MergeSubstreams();

            var drainingControl = KafkaConsumer.CommittableSource(consumerSettings, Subscriptions.Topics("akka-input"))
                .BackpressureAlert(LogLevel.WarningLevel, TimeSpan.FromMilliseconds(500))
                .IdleTimeout(TimeSpan.FromSeconds(10))
                .WithAttributes(Attributes.CreateName("CommitableSource"))
                .Via(mappingFlow)
                .Select(t => ProducerMessage.Single(new ProducerRecord<Null, string>($"{t.Topic}-done", t.Value),
                    t.CommitableOffset))
                .BackpressureAlert(LogLevel.WarningLevel, TimeSpan.FromMilliseconds(500))
                .Via(KafkaProducer.FlexiFlow<Null, string, ICommittableOffset>(producerSettings)).WithAttributes(Attributes.CreateName("FlexiFlow"))
                .Select(m => (ICommittable)m.PassThrough)
                .AlsoToMaterialized(Committer.Sink(committerSettings), DrainingControl<NotUsed>.Create)
                .To(Flow.Create<ICommittable>()
                    .Async()
                    .GroupedWithin(1000, TimeSpan.FromSeconds(1))
                    .Select(c => c.Count())
                    .Log("MsgCount").AddAttributes(Attributes.CreateLogLevels(LogLevel.InfoLevel))
                    .To(Sink.Ignore<int>()))
                .Run(materializer);
            
            actorSystem.Log.Info("Stream started");
            
            await actorSystem.WhenTerminated;
        }
    }
}