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

        private static readonly TimeSpan AssignmentTimeout = TimeSpan.FromSeconds(15);

        public static async Task<int> Main()
        {
            using var registryClient = CreateSchemaRegistryClient();
            using var admin = CreateAdmin();
            using var consumer = CreateConsumer(registryClient);
            using var producer = await CreateProducer(registryClient, AutomaticRegistrationBehavior.Always, TemporalBehavior.EpochMilliseconds); // Can also be TemporalBehavior.Iso8601

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

            var player1 = new Player
            {
                Id = Guid.NewGuid(),
                Nickname = "Todd Bonzalez",
                LastLogin = NodaTime.Instant.FromDateTimeOffset(DateTimeOffset.UtcNow),
            };

            await producer.ProduceAsync(Topic, new Message<Guid, Player>
            {
                Key = player1.Id,
                Value = player1,
            });

            var result = consumer.Consume();
            var playerOne = result.Message.Value;
            Console.WriteLine($"Received update for {playerOne.Nickname} with last login {playerOne.LastLogin}.");

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
