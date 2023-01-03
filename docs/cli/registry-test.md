Verify that a .NET type is compatible with a schema in the Schema Registry.

## Examples

#### Test that a type works with the latest version of a subject

```sh
dotnet avro registry-test --assembly ./out/Example.Models.dll --registry-url http://registry:8081 --subject example_subject --type Example.Models.ExampleModel
```

```
A deserializer cannot be created for ExampleModel: ExampleModel does not have a field or property that matches the Number field on ExampleModel.
```

## Options

`-a`, `--assembly`
:   The name of or path to an assembly to load (multiple space-separated values accepted).

`-t`, `--type`
:   The type to build a schema for.

`-c`, `--registry-config`
:   Configuration options to provide to the registry client (multiple space-separated key=value pairs accepted).

`-r`, `--registry-url`
:   The URL of the schema registry.

#### Resolve schema by ID

`-i`, `--id`
:   The ID of the schema.

#### Resolve schema by subject/version

`-s`, `--subject`
:   The subject of the schema.

`-v`, `--version`
:   The version of the schema.
