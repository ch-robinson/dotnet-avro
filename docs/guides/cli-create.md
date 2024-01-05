Creating schemas from complex .NET types is a time-saving way to get started with Avro. Chr.Avro recognizes most commonly used types and supports classes, structs, and enums, so it’s usually possible to get a working schema with no additional manipulation.

For detailed information about how types are matched to schemas, see the [types and conversions](../internals/mapping.md) documentation.

## Installing the CLI

If you haven’t already, install the Chr.Avro CLI:

```
$ dotnet tool install Chr.Avro.Cli --global
Tool 'chr.avro.cli' (version '10.2.0') was successfully installed.
```

After the CLI tool has been installed, you can invoke it using `dotnet avro`. If the install command fails, make sure you have the latest version of the [.NET Core SDK](https://dotnet.microsoft.com/download) installed.

## Using the CLI

To create a schema for a type, use the `create` command. You’ll need to provide the type’s full name as well as the path to a compiled assembly that contains it:

```
$ dotnet avro create --type ExampleNamespace.ExampleLibrary.ExampleClass --assembly bin/Debug/netstandard2.0/ExampleNamespace.ExampleLibrary.dll
{"name":"ExampleNamespace.ExampleLibrary.ExampleClass","type":"record","fields":[{"name":"ExampleProperty","type":"int"}]}
```

## Customizing generated schemas

The CLI ships with some convenience options:

*   The `--nullable-references` option causes all reference types to be written as nullable unions. This is useful when you prefer to keep .NET’s nullable semantics.

*   The `--enums-as-integers` option causes enums to be represented as `"int"` or `"long"` schemas instead of `"enum"` schemas.

Chr.Avro also recognizes [data contract attributes](https://docs.microsoft.com/en-us/dotnet/api/system.runtime.serialization.datacontractattribute), which can be used to customize names.

If you need to make more complicated modifications to a generated schema, you can customize the schema creation process in code:

```csharp
using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using System;

namespace Chr.Avro.Examples.SchemaCustomization
{
    public class ExampleClass
    {
        public int NumericProperty { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = new SchemaBuilder();
            var schema = builder.BuildSchema<ExampleClass>(); // a RecordSchema instance

            // do modifications here

            var writer = new JsonSchemaWriter();
            Console.WriteLine(writer.Write(schema));
        }
    }
}
```
