namespace Chr.Avro.NodaTimeExample
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Chr.Avro.Abstract;
    using Chr.Avro.Confluent;
    using Chr.Avro.NodaTimeExample.Infrastructure;
    using Chr.Avro.NodaTimeExample.Models;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;
    using global::Confluent.Kafka.Admin;
    using global::Confluent.Kafka.SyncOverAsync;
    using global::Confluent.SchemaRegistry;

    internal class Program
    {
        private const string BootstrapServers = "localhost:9092";
        private const string SchemaRegistries = "http://localhost:8081";
        private const string Topic = "noda-time-example";

        // The other variants of TemporalBehavior is also supported. Just change this const.
        private const TemporalBehavior UseTemporalBehavior = TemporalBehavior.EpochMilliseconds;

        public static async Task<int> Main()
        {
            using var registryClient = CreateSchemaRegistryClient();
            using var admin = CreateAdmin();
            using var consumer = CreateConsumer(registryClient);
            using var producer = await CreateProducer(registryClient, AutomaticRegistrationBehavior.Always, UseTemporalBehavior);

            Console.WriteLine($"Creating {Topic}...");
            await EnsureTopicExists(admin);

            consumer.Subscribe(Topic);

            var producedPlayer = new Player
            {
                Id = Guid.NewGuid(),
                Nickname = "Todd Bonzalez",
                LastLogin = NodaTime.Instant.FromDateTimeOffset(DateTimeOffset.UtcNow),
            };

            Console.WriteLine($"Producing player with NodaTime Instant {producedPlayer.LastLogin}");
            await producer.ProduceAsync(Topic, new Message<Guid, Player>
            {
                Key = producedPlayer.Id,
                Value = producedPlayer,
            });

            var result = consumer.Consume();

            Console.WriteLine($"Consumed player with NodaTime Instant {result.Message.Value.LastLogin}");

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

        private static IConsumer<Guid, Player> CreateConsumer(
            ISchemaRegistryClient registryClient)
        {
            var deserializerBuilder = new BinaryDeserializerBuilder(
                BinaryDeserializerBuilder.CreateDefaultCaseBuilders()
                    .Prepend(builder => new NodaTimeDeserializerBuilderCase()));

            return new ConsumerBuilder<Guid, Player>(
                new ConsumerConfig
                {
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    BootstrapServers = BootstrapServers,
                    EnableAutoCommit = false,
                    GroupId = $"noda-time-example-{Guid.NewGuid()}",
                })
                .SetAvroKeyDeserializer(registryClient)
                .SetValueDeserializer(new AsyncSchemaRegistryDeserializer<Player>(
                    registryClient,
                    deserializerBuilder).AsSyncOverAsync())
                .Build();
        }

        private static async Task<IProducer<Guid, Player>> CreateProducer(
            ISchemaRegistryClient registryClient,
            AutomaticRegistrationBehavior automaticRegistrationBehavior,
            TemporalBehavior temporalBehavior)
        {
            var schemaBuilder = new SchemaBuilder(
                SchemaBuilder.CreateDefaultCaseBuilders()
                    .Prepend(builder => new NodaTimeSchemaBuilderCase(temporalBehavior)));

            using var serializerBuilder = new SchemaRegistrySerializerBuilder(
                registryClient,
                schemaBuilder,
                serializerBuilder: new BinarySerializerBuilder(
                    BinarySerializerBuilder.CreateDefaultCaseBuilders()
                        .Prepend(builder => new NodaTimeAsStringSerializerBuilderCase())
                        .Prepend(builder => new NodaTimeAsTimestampSerializerBuilderCase())));

            var producerBuilder = new ProducerBuilder<Guid, Player>(
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
                SubjectNameStrategy.Topic.ConstructValueSubjectName(Topic),
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
    }
}
