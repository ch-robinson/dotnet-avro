namespace Chr.Avro.UnionTypeExample
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Chr.Avro.Abstract;
    using Chr.Avro.Confluent;
    using Chr.Avro.Serialization;
    using Chr.Avro.UnionTypeExample.Infrastructure;
    using Chr.Avro.UnionTypeExample.Models;
    using global::Confluent.Kafka;
    using global::Confluent.Kafka.Admin;
    using global::Confluent.Kafka.SyncOverAsync;
    using global::Confluent.SchemaRegistry;

    internal class Program
    {
        private const string BootstrapServers = "localhost:9092";
        private const string SchemaRegistries = "http://localhost:8081";
        private const string Topic = "union-type-example";

        private static readonly TimeSpan AssignmentTimeout = TimeSpan.FromSeconds(15);

        public static async Task<int> Main()
        {
            using var registryClient = CreateSchemaRegistryClient();
            using var admin = CreateAdmin();
            using var consumer = CreateConsumer(registryClient);
            using var producer = await CreateProducer(registryClient, AutomaticRegistrationBehavior.Always);

            Console.WriteLine($"Creating {Topic}...");
            await EnsureTopicExists(admin);

            consumer.Subscribe(Topic);

            foreach (var message in GetEventSequence())
            {
                await producer.ProduceAsync(Topic, message);

                var result = consumer.Consume();
                Console.WriteLine($"Received an {result.Message.Value.Event.GetType().Name} event for {result.Message.Key.OrderId}.");
            }

            return 0;
        }

        private static IAdminClient CreateAdmin()
        {
            return new AdminClientBuilder(
                new AdminClientConfig
                {
                    BootstrapServers = BootstrapServers,
                })
                .Build();
        }

        private static IConsumer<OrderKey, OrderEventRecord> CreateConsumer(
            ISchemaRegistryClient registryClient)
        {
            var deserializerBuilder = new BinaryDeserializerBuilder(
                BinaryDeserializerBuilder.CreateDefaultCaseBuilders()
                    .Prepend(builder => new OrderEventUnionDeserializerBuilderCase(builder)));

            return new ConsumerBuilder<OrderKey, OrderEventRecord>(
                new ConsumerConfig
                {
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    BootstrapServers = BootstrapServers,
                    EnableAutoCommit = false,
                    GroupId = $"union-type-example-{Guid.NewGuid()}",
                })
                .SetAvroKeyDeserializer(registryClient)
                .SetValueDeserializer(new AsyncSchemaRegistryDeserializer<OrderEventRecord>(
                    registryClient,
                    deserializerBuilder).AsSyncOverAsync())
                .Build();
        }

        private static async Task<IProducer<OrderKey, OrderEventRecord>> CreateProducer(
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior automaticRegistrationBehavior)
        {
            var schemaBuilder = new SchemaBuilder(
                SchemaBuilder.CreateDefaultCaseBuilders()
                    .Prepend(builder => new OrderEventUnionSchemaBuilderCase(builder)));

            using var serializerBuilder = new SchemaRegistrySerializerBuilder(
                registryClient,
                schemaBuilder,
                serializerBuilder: new BinarySerializerBuilder(
                    BinarySerializerBuilder.CreateDefaultCaseBuilders()
                        .Prepend(builder => new OrderEventUnionSerializerBuilderCase(builder))));

            var producerBuilder = new ProducerBuilder<OrderKey, OrderEventRecord>(
                new ProducerConfig
                {
                    BootstrapServers = BootstrapServers,
                });

            await producerBuilder.SetAvroKeySerializer(
                registryClient,
                SubjectNameStrategy.Topic.ConstructKeySubjectName(Topic),
                automaticRegistrationBehavior);

            await producerBuilder.SetAvroValueSerializer(
                serializerBuilder,
                SubjectNameStrategy.Topic.ConstructKeySubjectName(Topic),
                automaticRegistrationBehavior);

            return producerBuilder.Build();
        }

        private static ISchemaRegistryClient CreateSchemaRegistryClient()
        {
            return new CachedSchemaRegistryClient(
                new SchemaRegistryConfig
                {
                    Url = SchemaRegistries,
                });
        }

        private static async Task EnsureTopicExists(IAdminClient admin)
        {
            var metadata = admin.GetMetadata(Topic, TimeSpan.FromSeconds(15));

            if (!metadata.Topics.Any(m => m.Topic == Topic))
            {
                await admin.CreateTopicsAsync(
                    new[]
                    {
                        new TopicSpecification
                        {
                            Name = Topic,
                        },
                    });
            }
        }

        private static IEnumerable<Message<OrderKey, OrderEventRecord>> GetEventSequence()
        {
            var order1Key = new OrderKey { OrderId = Guid.NewGuid() };
            var order2Key = new OrderKey { OrderId = Guid.NewGuid() };
            var order3Key = new OrderKey { OrderId = Guid.NewGuid() };

            var product1Id = Guid.NewGuid();
            var product2Id = Guid.NewGuid();

            yield return new Message<OrderKey, OrderEventRecord>
            {
                Key = order1Key,
                Value = new OrderEventRecord
                {
                    Timestamp = DateTime.UtcNow,
                    Event = new OrderCreationEvent
                    {
                        LineItems = new[]
                        {
                            new OrderLineItem { ProductId = product1Id, Quantity = 1 },
                            new OrderLineItem { ProductId = product2Id, Quantity = 1 },
                        },
                    },
                },
            };

            yield return new Message<OrderKey, OrderEventRecord>
            {
                Key = order2Key,
                Value = new OrderEventRecord
                {
                    Timestamp = DateTime.UtcNow,
                    Event = new OrderCreationEvent
                    {
                        LineItems = new[]
                        {
                            new OrderLineItem { ProductId = product2Id, Quantity = 5 },
                        },
                    },
                },
            };

            yield return new Message<OrderKey, OrderEventRecord>
            {
                Key = order1Key,
                Value = new OrderEventRecord
                {
                    Timestamp = DateTime.UtcNow,
                    Event = new OrderLineItemModificationEvent
                    {
                        Index = 1,
                        LineItem = new OrderLineItem { ProductId = product2Id, Quantity = 10 },
                    },
                },
            };

            yield return new Message<OrderKey, OrderEventRecord>
            {
                Key = order3Key,
                Value = new OrderEventRecord
                {
                    Timestamp = DateTime.UtcNow,
                    Event = new OrderCreationEvent
                    {
                        LineItems = Array.Empty<OrderLineItem>(),
                    },
                },
            };

            yield return new Message<OrderKey, OrderEventRecord>
            {
                Key = order1Key,
                Value = new OrderEventRecord
                {
                    Timestamp = DateTime.UtcNow,
                    Event = new OrderLineItemModificationEvent
                    {
                        Index = 0,
                        LineItem = new OrderLineItem { ProductId = product1Id, Quantity = 5 },
                    },
                },
            };

            yield return new Message<OrderKey, OrderEventRecord>
            {
                Key = order3Key,
                Value = new OrderEventRecord
                {
                    Timestamp = DateTime.UtcNow,
                    Event = new OrderCancellationEvent { },
                },
            };
        }
    }
}
