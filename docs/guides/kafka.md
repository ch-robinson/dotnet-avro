Chr.Avro ships with first-class support for [Confluent’s Kafka clients](https://github.com/confluentinc/confluent-kafka-dotnet), the shortest path to creating Kafka producers and consumers in .NET.

## Using Confluent’s client builders

First, add a reference to the Chr.Avro.Confluent package:

```
$ dotnet add package Chr.Avro.Confluent --version 10.2.0
```

Chr.Avro.Confluent depends on [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka), which contains [producer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerBuilder-2.html) and [consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ConsumerBuilder-2.html) builders. To build a [Schema Registry](https://www.confluent.io/confluent-schema-registry/)-integrated producer, use the producer builder in tandem with Chr.Avro.Confluent’s Avro extension methods:

```csharp
using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.KafkaProducer
{
    public class ExampleValue
    {
        public string Property { get; set; }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = "broker1:9092,broker2:9092"
            };

            var registryConfig = new RegistryConfig()
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            using (var registry = new CachedSchemaRegistryClient(registryConfig))
            {
                var builder = new ProducerBuilder<Ignore, ExampleValue>(producerConfig)
                    .SetAvroValueSerializer(registry, registerAutomatically: AutomaticRegistrationBehavior.Always)
                    .SetErrorHandler((_, error) => Console.Error.WriteLine(error.ToString()));

                using (var producer = builder.Build())
                {
                    await producer.ProduceAsync("example_topic", new Message<Ignore, ExampleValue>
                    {
                        Value = new ExampleValue
                        {
                            Property = "example!"
                        }
                    });
                }
            }
        }
    }
}
```

The serializer assumes (per Confluent convention) that the value subject for `example_topic` is `example_topic-value`. (The key subject would be `example_topic-key`.) When messages are published, the serializer will attempt to pull down a schema from the Schema Registry. The serializer can be configured to generate and register a schema automatically if one doesn’t exist.

Building consumers works in a similar way—schemas will be retrieved from the Schema Registry as messages are consumed:

```csharp
using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;

namespace Chr.Avro.Examples.KafkaConsumer
{
    public class ExampleValue
    {
        public string Property { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = "broker1:9092,broker2:9092",
                GroupId = "example_consumer_group"
            };

            var registryConfig = new RegistryConfig()
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            using (var registry = new CachedSchemaRegistryClient(registryClient))
            {
                var builder = new ConsumerBuilder<Ignore, ExampleValue>(consumerConfig)
                    .SetAvroValueDeserializer(registry)
                    .SetErrorHandler((_, error) => Console.Error.WriteLine(error.ToString()));

                using (var consumer = builder.Build())
                {
                    consumer.Subscribe("example_topic");

                    while (true)
                    {
                        var result = consumer.Consume();
                        Console.WriteLine(result.Value.Property);
                    }
                }
            }
        }
    }
}
```
