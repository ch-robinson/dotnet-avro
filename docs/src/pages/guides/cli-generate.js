import { Link, graphql, useStaticQuery } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import ExternalLink from '../../components/site/external-link'

const title = 'Generating C# code from Avro schemas'

export default () => {
  const {
    site: {
      siteMetadata: { latestRelease, projectName }
    }
  } = useStaticQuery(graphql`
    query {
      site {
        siteMetadata {
          latestRelease
          projectName
        }
      }
    }
  `)

  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>

      <h1>{title}</h1>
      <p>{projectName} is capable of generating rudimentary C# class and enum definitions to match Avro’s record and enum schemas. If you have a complex Avro schema, but no matching .NET type, code generation can save a lot of time.</p>

      <h2>Getting started</h2>
      <p>If you haven’t already, install the {projectName} CLI:</p>
      <Highlight language='bash'>{`$ dotnet tool install Chr.Avro.Cli --global
Tool 'chr.avro.cli' (version '${latestRelease}') was successfully installed.`}</Highlight>
      <p>After the CLI tool has been installed, you can invoke it using <code>dotnet avro</code>. If the install command fails, make sure you have the latest version of the <ExternalLink to='https://dotnet.microsoft.com/download'>.NET Core SDK</ExternalLink> installed.</p>

      <h2>Using the CLI</h2>
      <p>To generate code for a schema, use the <Link to='/cli#generate'><Highlight inline language='bash'>generate</Highlight></Link> command. The CLI supports retrieving schemas from a Confluent <ExternalLink to='https://www.confluent.io/confluent-schema-registry/'>Schema Registry</ExternalLink>:</p>
      <Highlight language='bash'>{`$ dotnet avro generate --id 42 --registry-url http://registry:8081
namespace ExampleNamespace
{
    public class ExampleClass
    {
        public long LongProperty { get; set; }

        public string StringProperty { get; set; }
    }
}`}</Highlight>
      <p>The CLI writes generated code to the <ExternalLink to='https://en.wikipedia.org/wiki/Standard_streams#Standard_output_(stdout)'>console</ExternalLink>. Use your shell’s capabilities to read from and write to files. In Bash, that looks like this:</p>
      <Highlight language='bash'>{'$ dotnet avro generate < example-class.avsc > ExampleClass.cs'}</Highlight>
      <p>And in PowerShell:</p>
      <Highlight language='powershell'>{'PS C:\\> Get-Content .\\example-class.avsc | dotnet avro generate | Out-File .\\ExampleClass.cs'}</Highlight>

      <p>Generated enums and classes are grouped by namespace.</p>
    </>
  )
}
