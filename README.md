# Chr.Avro

Chr.Avro is an Avro implementation for .NET. It’s designed to serve as a flexible alternative to the [Apache implementation](https://github.com/apache/avro/tree/master/lang/csharp/src/apache/main) and integrate seamlessly with [Confluent’s Kafka and Schema Registry clients](https://github.com/confluentinc/confluent-kafka-dotnet).

*   [Chr.Avro](src/Chr.Avro): schema models, type resolution, and schema builder
*   [Chr.Avro.Binary](src/Chr.Avro.Binary): binary serialization implementation
*   [Chr.Avro.Cli](src/Chr.Avro.Cli): command line interface
*   [Chr.Avro.Codegen](src/Chr.Avro.Codegen): experimental C# code generation
*   [Chr.Avro.Confluent](src/Chr.Avro.Confluent): serializers/deserializers and glue
*   [Chr.Avro.Json](src/Chr.Avro.Json): JSON schema representation

**For more information, check out [the documentation](https://ch-robinson.github.io/dotnet-avro/).**

## Quick start

**To use the command line interface:** Install [Chr.Avro.Cli](https://www.nuget.org/packages/Chr.Avro.Cli) as a [global tool](https://docs.microsoft.com/en-us/dotnet/core/tools/global-tools):

```
$ dotnet tool install Chr.Avro.Cli --global --version 1.0.0-rc.0
You can invoke the tool using the following command: dotnet-avro
Tool 'chr.avro.cli' (version '1.0.0-rc.0') was successfully installed.
$ dotnet avro help
Chr.Avro 1.0.0-rc.0
...
```

**To use the Kafka producer/consumer builders in your project:** Add [Confluent.Kafka](https://www.nuget.org/packages/Confluent.Kafka) and [Chr.Avro.Confluent](https://www.nuget.org/packages/Chr.Avro.Confluent) as project dependencies. After that, check out [this guide](https://ch-robinson.github.io/dotnet-avro/guides/kafka/) or read on for some other examples.

## Examples

The CLI can be used to generate Avro schemas for .NET types (both built-in and from compiled assemblies):

```
$ dotnet avro create -t System.Int32
"int"
$ dotnet avro create -t System.Decimal
{"type":"bytes","logicalType":"decimal","precision":29,"scale":14}
$ dotnet avro create -a out/example.dll -t ExampleRecord
{"name":"ExampleRecord","type":"record","fields":[{"name":"Number","type":"long"}]}
```

It can also verify that a .NET type can be mapped to a [Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html) schema (useful for both development and CI):

```
$ dotnet avro registry-test -a out/example.dll -t ExampleRecord -r http://registry:8081 -i 242
A deserializer cannot be created for ExampleRecord: ExampleRecord does not have a field or property that matches the correlation_id field on example_record.
```

`SchemaRegistryProducerBuilder` and `SchemaRegistryConsumerBuilder` create Kafka clients to produce and consume CLR objects:

```csharp
using Chr.Avro.Confluent;
using System;

namespace Example
{
    class ExampleRecord
    {
        public Guid CorrelationId { get; set; }
        public DateTime Timestamp { get; set; }
    }

    class Program
    {
        const string Brokers = "broker1:9092,broker2:9092";
        const string Group = "example_consumer_group";
        const string RegistryUrl = "http://registry:8081";

        static void Main(string[] args)
        {
            using (var builder = new SchemaRegistryConsumerBuilder(Brokers, Group, RegistryUrl))
            using (var consumer = builder.BuildConsumer<string, ExampleRecord>())
            {
                var result = consumer.Consume(CancellationToken.None);
                Console.WriteLine($"Consumed message! {result.Key}: {result.Value.Timestamp}");
            }
        }
    }
}
```

Under the hood, `SchemaBuilder` is responsible for generating schemas from CLR types:

```csharp
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using System;

namespace Example
{
    enum Fear
    {
        Bears,
        Children,
        Haskell,
    }

    struct FullName
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    class Person
    {
        public Guid Id { get; set; }
        public Fear GreatestFear { get; set; }
        public FullName Name { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var builder = new SchemaBuilder();
            var writer = new JsonSchemaWriter();

            Console.WriteLine(writer.Write(builder.BuildSchema<double>));
            // "double"

            Console.WriteLine(writer.Write(builder.BuildSchema<DateTime>));
            // "string"

            Console.WriteLine(writer.Write(builder.BuildSchema<Fear>));
            // {"name":"Fear","type":"enum","symbols":["Bears","Children","Haskell"]}

            Console.WriteLine(writer.Write(builder.BuildSchema<Person>));
            // {"name":"Person","type":"record"...}
        }
    }
}
```

## Contributing

[Cake](https://cakebuild.net) handles all build tasks. Use [**build.ps1**](build.ps1) on Windows and [**build.sh**](build.sh) on macOS and Linux. (Some projects target .NET Framework 4.5.2, so certain tasks won’t work on non-Windows machines.)

The following targets are supported:

| Name        | Description                                     |
|-------------|------------------------------------------------ |
| **Build**   | builds the library projects                     |
| **Clean**   | removes all build and release artifacts         |
| **Pack**    | creates NuGet packages for the library projects |
| **Publish** | pushes packages to NuGet                        |
| **Test**    | runs the test projects                          |

**Build** and **Test** will run if no target is specified.
