Chr.Avro is capable of generating rudimentary C# class and enum definitions to match Avro’s record and enum schemas. If you have a complex Avro schema, but no matching .NET type, code generation can save a lot of time.

## Installing the CLI

If you haven’t already, install the Chr.Avro CLI:

```
$ dotnet tool install Chr.Avro.Cli --global
Tool 'chr.avro.cli' (version '10.2.0') was successfully installed.
```

After the CLI tool has been installed, you can invoke it using `dotnet avro`. If the install command fails, make sure you have the latest version of the [.NET Core SDK](https://dotnet.microsoft.com/download) installed.

## Using the CLI

To generate code for a schema, use the `generate` command. The CLI supports retrieving schemas from a Confluent [Schema Registry](https://www.confluent.io/confluent-schema-registry/):

```
$ dotnet avro generate --id 42 --registry-url http://registry:8081
namespace ExampleNamespace
{
    public class ExampleClass
    {
        public long LongProperty { get; set; }
        public string StringProperty { get; set; }
    }
}
```

The CLI writes generated code to the [console](https://en.wikipedia.org/wiki/Standard_streams#Standard_output_(stdout)). Use your shell’s capabilities to read from and write to files. In Bash, that looks like this:

```bash
dotnet avro generate < example-class.avsc > ExampleClass.cs
```

And in PowerShell:

```pwsh
Get-Content .\example-class.avsc | dotnet avro generate | Out-File .\ExampleClass.cs
```

Generated enums and classes are grouped by namespace.
