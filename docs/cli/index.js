'use strict'

// eventually, figure out a way to extract this data programmatically

const clrTypeOptions = [{
  abbreviation: 'a',
  name: 'assembly',
  required: false,
  summary: 'The name of or path to an assembly to load (multiple space-separated values accepted).'
}, {
  name: 'enums-as-ints',
  required: false,
  summary: 'Whether enums should be represented as integers.'
}, {
  name: 'nullable-references',
  required: false,
  summary: 'Whether reference types should be nullable.'
}, {
  name: 'temporal-behavior',
  required: false,
  summary: 'Whether timestamps should be represented with "string" schemas (ISO 8601) or "long" schemas (timestamp logical types). Options are iso8601, epochmilliseconds, and epochmicroseconds.'
}, {
  abbreviation: 't',
  name: 'type',
  required: true,
  summary: 'The type to build a schema for.'
}]

const schemaResolutionOptions = [{
  abbreviation: 'r',
  name: 'registry-url',
  required: true,
  summary: 'The URL of the schema registry.'
}, {
  abbreviation: 'i',
  name: 'id',
  required: true,
  set: 'Resolve schema by ID',
  summary: 'The ID of the schema.'
}, {
  abbreviation: 's',
  name: 'subject',
  required: true,
  set: 'Resolve schema by subject/version',
  summary: 'The subject of the schema.'
}, {
  abbreviation: 'v',
  name: 'version',
  set: 'Resolve schema by subject/version',
  summary: 'The version of the schema.'
}]

module.exports = [{
  name: 'create',
  summary: 'Create an Avro schema for a .NET type.',
  examples: [{
    title: 'Create a schema for a built-in type',
    body: `$ dotnet avro create --type System.DateTime
"string"`
  }, {
    title: 'Create a schema for a type in a compiled assembly',
    body: `$ dotnet avro create --assembly ./out/Example.Models.dll --type Example.Models.ExampleModel
{"name":"Example.Models.ExampleModel",type":"record",fields:[{"name":"Text","type":"string"}]}`
  }],
  options: [...clrTypeOptions]
}, {
  name: 'generate',
  summary: 'Generates C# code for a schema from the Schema Registry.',
  examples: [{
    title: 'Generate code for a schema by ID',
    body: `$ dotnet avro generate --id 120 --registry-url http://registry:8081
namespace Example.Models
{
    public class ExampleModel
    {
        public string Text { get; set; }
    }
}`
  }, {
    title: 'Generate code from a local schema file (Bash)',
    body: `$ dotnet avro generate < example-model.avsc > ExampleModel.cs`
  }, {
    title: 'Generate code from a local schema file (PowerShell)',
    body: `PS C:\\> Get-Content .\\example-model.avsc | dotnet avro generate | Out-File .\\ExampleModel.cs`,
    language: 'powershell'
  }],
  options: [...schemaResolutionOptions]
}, {
  name: 'registry-get',
  summary: 'Retrieve a schema from the Schema Registry.',
  examples: [{
    title: 'Get a schema by ID',
    body: `$ dotnet avro registry-get --id 120 --registry-url http://registry:8081
{"name":"Example.Models.ExampleModel",type":"record",fields:[{"name":"Text","type":"string"}]}`,
  }],
  options: [...schemaResolutionOptions]
}, {
  name: 'registry-test',
  summary: 'Verify that a .NET type is compatible with a schema in the Schema Registry.',
  examples: [{
    title: 'Test that a type works with the latest version of a subject',
    body: `$ dotnet avro registry-test --assembly ./out/Example.Models.dll --registry-url http://registry:8081 --subject example_subject --type Example.Models.ExampleModel
A deserializer cannot be created for ExampleModel: ExampleModel does not have a field or property that matches the Number field on ExampleModel.`
  }],
  options: [...clrTypeOptions, ...schemaResolutionOptions]
}]
