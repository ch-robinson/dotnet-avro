import { Link } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import ExternalLink from '../../components/site/external-link'

const title = 'Generating C# code from Avro schemas'

export default () =>
  <>
    <Helmet>
      <title>{title}</title>
    </Helmet>

    <h1>{title}</h1>
    <p>Chr.Avro is capable of generating rudimentary C# class and enum definitions to match Avro’s record and enum schemas. If you have a complex Avro schema, but no matching .NET type, code generation can save a lot of time.</p>

    <h2>Getting started</h2>
    <p>If you haven’t already, install the Chr.Avro CLI:</p>
    <Highlight language='shell'>{`$ dotnet tool install Chr.Avro.Cli --global --version 1.0.0-rc.6
Tool 'chr.avro.cli' (version '1.0.0-rc.6') was successfully installed.`}</Highlight>
    <p>After the CLI tool has been installed, you can invoke it using <code>dotnet avro</code>. If the install command fails, make sure you have the latest version of the <ExternalLink to='https://dotnet.microsoft.com/download'>.NET Core SDK</ExternalLink> installed.</p>

    <h2>Using the CLI</h2>
    <p>To generate code for a schema, use the <Link to='/cli#generate'><Highlight inline language='shell'>generate</Highlight></Link> command:</p>
    <Highlight language='shell'>{`$ dotnet avro generate --id 42 --registry-url http://registry:8081
namespace ExampleNamespace
{
    public class ExampleClass
    {
        public long LongProperty { get; set; }

        public string StringProperty { get; set; }
    }
}`}</Highlight>
    <p>Generated enums and classes are grouped by namespace. In the future, it may be possible to customize generated names and write out results to individual files.</p>
  </>
