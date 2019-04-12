import { Link } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import DotnetReference from '../../components/references/dotnet'
import ExternalLink from '../../components/site/external-link'

const title = 'Creating schemas from .NET types'

export default () =>
  <>
    <Helmet>
      <title>{title}</title>
    </Helmet>

    <h1>{title}</h1>
    <p>Creating schemas from complex .NET types is a time-saving way to get started with Avro. Chr.Avro recognizes most commonly used types and supports classes, structs, and enums, so it’s usually possible to get a working schema with no additional manipulation.</p>
    <p>For detailed information about how types are matched to schemas, see the <Link to='/internals/mapping'>types and conversions</Link> documentation.</p>

    <h2>Getting started</h2>
    <p>If you haven’t already, install the Chr.Avro CLI:</p>
    <Highlight language='shell'>{`$ dotnet tool install Chr.Avro.Cli --global --version 1.0.0-rc.3
Tool 'chr.avro.cli' (version '1.0.0-rc.3') was successfully installed.`}</Highlight>
    <p>After the CLI tool has been installed, you can invoke it using <code>dotnet avro</code>. If the install command fails, make sure you have the latest version of the <ExternalLink to='https://dotnet.microsoft.com/download'>.NET Core SDK</ExternalLink> installed.</p>

    <h2>Using the CLI</h2>
    <p>To create a schema for a type, use the <Link to='/cli#create'><Highlight inline language='shell'>create</Highlight></Link> command. You’ll need to provide the type’s full name as well as the path to a compiled assembly that contains it:</p>
    <Highlight language='shell'>{`$ dotnet avro create --type ExampleNamespace.ExampleLibrary.ExampleClass --assembly bin/Debug/netstandard2.0/ExampleNamespace.ExampleLibrary.dll
{"name":"ExampleNamespace.ExampleLibrary.ExampleClass","type":"record","fields":[{"name":"ExampleProperty","type":"int"}]}`}</Highlight>

    <h2>Customizing generated schemas</h2>
    <p>The CLI ships with some convenience options:</p>
    <ul>
      <li>
        <p>The <strong><code>--nullable-references</code></strong> option causes all reference types to be written as nullable unions. This is useful when you prefer to keep .NET’s nullable semantics.</p>
      </li>
      <li>
        <p>The <strong><code>--enums-as-integers</code></strong> option causes enums to be represented as <Highlight inline language='avro'>"int"</Highlight> or <Highlight inline language='avro'>"long"</Highlight> schemas instead of <Highlight inline language='avro'>"enum"</Highlight> schemas.</p>
      </li>
    </ul>
    <p>Chr.Avro also recognizes <DotnetReference id='T:System.Runtime.Serialization.DataContractAttribute'>data contract attributes</DotnetReference>, which can be used to customize names.</p>
    <p>If you need to make more complicated modifications to a generated schema, you can customize the schema creation process in code:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Abstract;
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
`}</Highlight>
  </>
