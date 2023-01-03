Retrieve a schema from the Schema Registry.

## Examples

#### Get a schema by ID

```sh
dotnet avro registry-get --id 120 --registry-url http://registry:8081
```

```json
{"name":"Example.Models.ExampleModel",type":"record",fields:[{"name":"Text","type":"string"}]}
```

## Options

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
