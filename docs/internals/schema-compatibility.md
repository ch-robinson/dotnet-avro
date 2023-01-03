Avro schemas are often designed with future evolution in mind: When a schema is updated, it’s generally preferable that consumers should be able to continue handling messages. One high-level goal of Chr.Avro is to facilitate flexible and predictable schema update processes.

## Schema Registry compatibility levels

Confluent’s [Schema Registry](https://github.com/confluentinc/schema-registry) enforces [compatibility checks](https://docs.confluent.io/current/schema-registry/docs/avro.html) for schemas that it manages. Schema owners can choose one of these compatibility types:

*   `NONE`: No compatibility guaranteed.
*   `BACKWARD` and `BACKWARD_TRANSITIVE`: A reader using a newer schema version must be able to read data written using older schema versions.
*   `FORWARD` and `FORWARD_TRANSITIVE`: A reader using an older schema version must be able to read data written using newer schema versions.
*   `FULL` and `FULL_TRANSITIVE`: Backward and forward compatibility is guaranteed.

If a subject is configured for `BACKWARD` compatibility (the default), each new version can only make backward-compatible changes. These compatibility types are only used by the registry, not by Chr.Avro or other Avro libraries. (Chr.Avro only concerns itself with whether the serializer builder and deserializer builder can [map a schema to a .NET type](./mapping.md).)

## Producer and consumer implementations

When building producers and consumers with Chr.Avro.Confluent, there are two ways to configure Avro serialization and deserialization:

*   **On the fly (async):** Schemas are retrieved from the Schema Registry as needed. When producing, the serializer will derive a subject name from the topic being produced to (e.g., `topic_name-key` or `topic_name-value`) and optionally register a matching schema. When consuming, the deserializer will look up the schema [by ID](https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format). Serializers and deserializers are not bound to a specific schema.
*   **Bound at startup (sync):** Serializers and deserializers are created for a specific schema.

Chr.Avro.Confluent provides some extension methods to configure the Confluent.Kafka [producer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerBuilder-2.html) and [consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ConsumerBuilder-2.html) builders for Avro serialization. In the example below, a producer is created with serializers bound at startup:

```csharp
using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.ConfluentProducer
{
    public class Person
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "broker1:9092,broker2:9092"
            };

            var registryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            var builder = new ProducerBuilder<Guid, Person>(producerConfig);

            using (var registry = new CachedSchemaRegistryClient(registryConfig))
            {
                await Task.WhenAll(
                    builder.SetAvroKeySerializer(registry, "person-key", registerAutomatically: AutomaticRegistrationBehavior.Always),
                    builder.SetAvroValueSerializer(registry, "person-value", registerAutomatically: AutomaticRegistrationBehavior.Always)
                );
            }

            using (var producer = builder.Build())
            {
                // produce
            }
        }
    }
}
```

Binding at startup is generally good practice for producers. If a type mapping exception is thrown, it will be thrown before the producer is created and any messages are produced. For consumers, on the other hand, it’s generally better to resolve schemas on the fly—the deserializer will be able to handle updates to the schema as long as the type can be mapped. The following example demonstrates how to configure async deserializers:

```csharp
using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.ConfluentConsumer
{
    public class Person
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "broker1:9092,broker2:9092",
                GroupId = "example_group"
            };

            var registryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            var builder = new ConsumerBuilder<Guid, Person>(consumerConfig);

            using (var registry = new CachedSchemaRegistryClient(registryConfig))
            {
                builder.SetAvroKeyDeserializer(registry);
                builder.SetAvroValueDeserializer(registry);

                using (var consumer = builder.Build())
                {
                    // consume
                }
            }
        }
    }
}
```
