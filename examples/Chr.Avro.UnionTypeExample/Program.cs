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
    using Chr.Avro.UnionTypeExample.Cases;
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
            var assignmentSignal = new CancellationTokenSource(AssignmentTimeout);

            Console.WriteLine($"Subscribing to {Topic}...");

            while (consumer.Assignment.Count < 1)
            {
                if (assignmentSignal.IsCancellationRequested)
                {
                    Console.Error.WriteLine($"Failed to receive partition assigment for {Topic} within {AssignmentTimeout.TotalSeconds} seconds.");
                    return 1;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }

            var message = await producer.ProduceAsync(
                Topic,
                new Message<Null, MyMessage>
                {
                    Value = new MyMessage
                    {
                        Name = "TestName",
                        DateTime = DateTime.Now,
                        Payload = new Dictionary<string, IDataObj>
                        {
                            {
                                "Obj1", new DataObj1
                                {
                                    Name = "Obj1",
                                    Value = 123,
                                }
                            },
                            {
                                "Obj2", new DataObj2
                                {
                                    Name = "Obj2",
                                    Value = true,
                                    Foo = 456,
                                }
                            },
                            {
                                "Obj3", new DataObj3
                                {
                                    Name = "Obj3",
                                    Value = 7.89,
                                }
                            },
                        },
                    },
                });

            var result = consumer.Consume();
            Console.WriteLine($"{result.Message.Value.Name} {result.Message.Value.DateTime}");

            return 0;
        }

        private static IAdminClient CreateAdmin()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = BootstrapServers,
            };

            return new AdminClientBuilder(config)
                .Build();
        }

        private static IConsumer<Ignore, MyMessage> CreateConsumer(
            ISchemaRegistryClient registryClient)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                EnableAutoCommit = false,
                GroupId = $"union-type-example-{Guid.NewGuid()}",
            };

            var deserializerBuilder = new BinaryDeserializerBuilder(
                BinaryDeserializerBuilder.CreateDefaultCaseBuilders()
                    .Prepend(builder => new CustomUnionDeserializerBuilderCase(builder)));

            return new ConsumerBuilder<Ignore, MyMessage>(config)
                .SetValueDeserializer(new AsyncSchemaRegistryDeserializer<MyMessage>(
                    registryClient,
                    deserializerBuilder).AsSyncOverAsync())
                .Build();
        }

        private static async Task<IProducer<Null, MyMessage>> CreateProducer(
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior automaticRegistrationBehavior)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = BootstrapServers,
            };

            var schemaBuilder = new SchemaBuilder(
                SchemaBuilder.CreateDefaultCaseBuilders()
                    .Prepend(builder => new CustomUnionSchemaBuilderCase(builder)));

            using var serializerBuilder = new SchemaRegistrySerializerBuilder(
                registryClient,
                schemaBuilder,
                serializerBuilder: new BinarySerializerBuilder(
                    BinarySerializerBuilder.CreateDefaultCaseBuilders()
                        .Prepend(builder => new CustomUnionSerializerBuilderCase(builder))));

            return (await new ProducerBuilder<Null, MyMessage>(config)
                .SetAvroValueSerializer(
                    serializerBuilder,
                    SubjectNameStrategy.Topic.ConstructValueSubjectName(Topic),
                    automaticRegistrationBehavior))
                .Build();
        }

        private static ISchemaRegistryClient CreateSchemaRegistryClient()
        {
            var config = new SchemaRegistryConfig
            {
                Url = SchemaRegistries,
            };

            return new CachedSchemaRegistryClient(config);
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
    }
}
