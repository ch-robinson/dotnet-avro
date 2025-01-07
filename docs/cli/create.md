Create an Avro schema for a .NET type.

## Examples

#### Create a schema for a built-in type

```sh
dotnet avro create --type System.DateTime
```

```json
"string"
```

#### Create a schema for a type in a compiled assembly

```sh
dotnet avro create --assembly ./out/Example.Models.dll --type Example.Models.ExampleModel
```

```json
{"name":"Example.Models.ExampleModel",type":"record",fields:[{"name":"Text","type":"string"}]}
```

## Options

`-a`, `--assembly`
:   The name of or path to an assembly to load (multiple space-separated values accepted).

`-t`, `--type`
:   The type to build a schema for.

`--enum-behavior`
:   The type of schema that enum types should be represented by. Options are `symbolic` (generate an `"enum"` schema; the default behavior), `integral` (generate an `"int"` or `"long"` schema based on the underlying type; the behavior for all flag enums), and `nominal` (generate a `"string"` schema).

`--nullable-references`
:   Whether reference types selected for nullable record fields should be annotated as nullable.

`--temporal-behavior`
:   Whether timestamps should be represented with `"string"` schemas (ISO 8601) or `"long"` schemas (timestamp logical types). Options are `iso8601`, `epochmilliseconds`, `epochmicroseconds`, and `epochnanoseconds`.
