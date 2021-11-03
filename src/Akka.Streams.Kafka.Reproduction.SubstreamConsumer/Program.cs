﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Aaron.Akka.Streams.Dsl;
using Akka.Actor;
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

            var consumerSettings = ConsumerSettings<Null, string>
                .Create(actorSystem, null, null)
                .WithBootstrapServers("localhost:29092")
                .WithStopTimeout(TimeSpan.Zero)
                .WithGroupId("group1");
            
            var producerSettings = ProducerSettings<Null, string>.Create(actorSystem,
                    null, null)
                .WithBootstrapServers("localhost:29092");
            
            // TODO: we should just be able to accept a `ConsumerConfig` property
            consumerConfig.ForEach(kv => consumerSettings = consumerSettings.WithProperty(kv.Key, kv.Value));

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

            Console.ReadLine();
            await drainingControl.DrainAndShutdown();
            await actorSystem.Terminate();
        }
    }
}