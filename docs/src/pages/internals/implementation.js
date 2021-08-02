import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import DotnetReference from '../../components/references/dotnet'
import ExternalLink from '../../components/site/external-link'

const title = 'Implementation differences'

export default function ImplementationPage () {
  return (
    <>
      <Helmet>
        <title>{title}</title>
      </Helmet>

      <h1>{title}</h1>
      <p>Chr.Avro was created as a flexible alternative to Apache’s C# Avro implementation. This document explains the rationale for the creation of an entirely new library and outlines some of the differences between Chr.Avro and other implementations.</p>

      <h2>Schema representation</h2>
      <p>The main architectural difference between Chr.Avro and other implementations is its abstract schema representation. One of the main drawbacks of the Apache implementation is that its schema respresentations are inextricably bound to JSON. The <DotnetReference id='M:Avro.Schema.Parse(System.String)'><Highlight inline language='csharp'>Parse</Highlight> method on the <Highlight inline language='csharp'>Schema</Highlight> class</DotnetReference> is the only publicly exposed factory method, which means that it’s impossible to manipulate a schema without manipulating JSON. Chr.Avro keeps its abstract, binary, and JSON components entirely separate.</p>

      <h2>Development activity</h2>
      <p>The Apache implementation is minimally maintained. Small changes are contributed occasionally; the last major changes were <ExternalLink to='https://github.com/apache/avro/commits/master/lang/csharp/src/apache'>years ago</ExternalLink>. There doesn’t appear to be any appetite for major changes. <ExternalLink to='https://azure.microsoft.com/en-us/blog/microsoft-avro-library/'>Microsoft.Hadoop.Avro</ExternalLink> (<ExternalLink to='https://github.com/dougmsft/microsoft-avro'>Microsoft.Avro.Core</ExternalLink>?) was the only other open source Avro implementation for .NET. It’s been abandoned since 2016.</p>

      <h2>Type mapping</h2>
      <p>The Microsoft implementation made it extremely easy to map Avro records to existing .NET classes, something that Chr.Avro has aimed to imitate. The Apache implementation does not map to existing classes. Instead, users are given a choice between two less flexible options:</p>
      <ul>
        <li>
          <p>Use the <DotnetReference id='T:Avro.Generic.GenericRecord'><Highlight inline language='csharp'>GenericRecord</Highlight> class</DotnetReference>, essentially an untyped dictionary. This approach offers no compile-time guarantees.</p>
        </li>
        <li>
          <p>Use the <ExternalLink to='https://github.com/confluentinc/confluent-kafka-dotnet#working-with-apache-avro'>avrogen</ExternalLink> tool to generate classes that implement <DotnetReference id='T:Avro.Specific.ISpecificRecord'><Highlight inline language='csharp'>ISpecificRecord</Highlight></DotnetReference>. While the generated classes offer some compile-time safety, the process is cumbersome, and additional work usually has to be done to map the generated classes to actual model classes.</p>
        </li>
      </ul>

      <h2>Undefined behaviors</h2>
      <p>The Avro specification leaves certain behaviors undefined, and in some cases Chr.Avro implements them differently than other libraries. None of these differences are correctness issues—all serialized payloads are correct, and all correct payloads can be deserialized.</p>

      <h3>Block sizes</h3>
      <p>Avro encodes arrays and maps as a series of blocks terminated by an empty block. For example, an array of length 20 could be encoded as 4 blocks with lengths 6, 10, 4, and 0. Chr.Avro doesn’t make any effort to break arrays and maps into chunks; instead, it always encodes all non-empty arrays and maps as two blocks (the first full-length, the second zero-length). This is consistent with most other implementations.</p>

      <h3>Invalid boolean values</h3>
      <p>Avro specifies that booleans should be encoded as a single byte: <Highlight inline language='csharp'>0x00</Highlight> (false) or <Highlight inline language='csharp'>0x01</Highlight> (true). If a value greater than <Highlight inline language='csharp'>0x01</Highlight> is encountered, Chr.Avro decodes the value as true.</p>
      <p>The <ExternalLink to='https://github.com/apache/avro/blob/master/lang/java/avro/src/main/java/org/apache/avro/io/BinaryDecoder.java#L133'>Apache Java implementation</ExternalLink> decodes all non-<Highlight inline language='csharp'>0x01</Highlight> values as false. The <ExternalLink to='https://github.com/apache/avro/blob/master/lang/csharp/src/apache/main/IO/BinaryDecoder.cs#L53'>Apache C# implementation</ExternalLink> throws an exception if a value other than <Highlight inline language='csharp'>0x00</Highlight> or <Highlight inline language='csharp'>0x01</Highlight> is encountered.</p>
    </>
  )
}
