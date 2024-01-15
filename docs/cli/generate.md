Generates C# code for a schema from the Schema Registry.

## Examples

#### Generate code for a schema by ID

```sh
dotnet avro generate --id 120 --registry-url http://registry:8081
```

```csharp
namespace Example.Models
{
    public class ExampleModel
    {
        public string Text { get; set; }
    }
}
```

#### Generate code for a schema by ID, connecting to the Schema Registry using basic auth

```sh
dotnet avro generate --id 120 --registry-url http://registry:8081 --registry-config schema.registry.basic.auth.user.info=exampleuser:password
```

```csharp
namespace Example.Models
{
    public class ExampleModel
    {
        public string Text { get; set; }
    }
}
```

#### Generate code from a local schema

=== "Bash"

    ```sh
    dotnet avro generate < example-model.avsc > ExampleModel.cs
    ```

=== "PowerShell"

    ```pwsh
    Get-Content .\example-model.avsc | dotnet avro generate | Out-File .\ExampleModel.cs
    ```

## Options

`-c`, `--registry-config`
:   Configuration options to provide to the registry client (multiple space-separated key=value pairs accepted).

`-r`, `--registry-url`
:   The URL of the schema registry.

`--component-model-annotations`
:   Whether to emit component model annotations for record and enum descriptions.

`--nullable-references`
:   Whether reference types selected for nullable record fields should be annotated as nullable.

#### Resolve schema by ID

`-i`, `--id`
:   The ID of the schema.

#### Resolve schema by subject/version

`-s`, `--subject`
:   The subject of the schema.

`-v`, `--version`
:   The version of the schema.
