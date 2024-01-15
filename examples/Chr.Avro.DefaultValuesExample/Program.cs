namespace Chr.Avro.DefaultValuesExample
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using Chr.Avro.Abstract;
    using Chr.Avro.Confluent;
    using Chr.Avro.DefaultValuesExample.Models;
    using Chr.Avro.Representation;
    using global::Confluent.Kafka;
    using global::Confluent.Kafka.Admin;
    using global::Confluent.SchemaRegistry;

    using Schema = global::Confluent.SchemaRegistry.Schema;

    internal class Program
    {
        private const string BootstrapServers = "localhost:9092";
        private const string SchemaRegistries = "http://localhost:8081";
        private const string Topic = "default-values-example";

        public static async Task<int> Main()
        {
            using var registryClient = new CachedSchemaRegistryClient(
                new SchemaRegistryConfig
                {
                    Url = SchemaRegistries,
                });

            using var admin = CreateAdmin();
            using var consumer = CreateConsumer(registryClient);
            using var producer = await CreateProducer(registryClient);

            Console.WriteLine($"Creating {Topic}...");
            await EnsureTopicExists(admin);

            consumer.Subscribe(Topic);

            var playerV2 = new PlayerV2
            {
                Id = Guid.NewGuid(),
                Nickname = "Todd Bonzalez",
            };

            await producer.ProduceAsync(Topic, new Message<Guid, PlayerV2>
            {
                Key = playerV2.Id,
                Value = playerV2,
            });

            var result = consumer.Consume();
            var playerV1 = result.Message.Value;
            Console.WriteLine($"Received update for {playerV1.Nickname} with health {playerV1.Health}.");

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

        private static IConsumer<Guid, PlayerV1> CreateConsumer(
            ISchemaRegistryClient registryClient)
        {
            return new ConsumerBuilder<Guid, PlayerV1>(
                new ConsumerConfig
                {
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    BootstrapServers = BootstrapServers,
                    EnableAutoCommit = false,
                    GroupId = $"union-type-example-{Guid.NewGuid()}",
                })
                .SetAvroKeyDeserializer(registryClient)
                .SetAvroValueDeserializer(registryClient)
                .Build();
        }

        private static async Task<IProducer<Guid, PlayerV2>> CreateProducer(
            ISchemaRegistryClient registryClient)
        {
            var schemaBuilder = new SchemaBuilder();
            var schemaWriter = new JsonSchemaWriter();

            var keySchemaId = await registryClient.RegisterSchemaAsync(
                SubjectNameStrategy.Topic.ConstructKeySubjectName(Topic),
                new Schema(schemaWriter.Write(schemaBuilder.BuildSchema<Guid>()), SchemaType.Avro));

            var valueSchemaId = await registryClient.RegisterSchemaAsync(
                SubjectNameStrategy.Topic.ConstructValueSubjectName(Topic),
                new Schema(schemaWriter.Write(schemaBuilder.BuildSchema<PlayerV1>()), SchemaType.Avro));

            var producerBuilder = new ProducerBuilder<Guid, PlayerV2>(
                new ProducerConfig
                {
                    BootstrapServers = BootstrapServers,
                });

            await producerBuilder.SetAvroKeySerializer(registryClient, keySchemaId);
            await producerBuilder.SetAvroValueSerializer(registryClient, valueSchemaId);

            return producerBuilder.Build();
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
