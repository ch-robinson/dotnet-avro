import { Link } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import DotnetReference from '../../components/references/dotnet'
import ExternalLink from '../../components/site/external-link'

const title = 'Schema compatibility'

export default () =>
  <>
    <Helmet>
      <title>{title}</title>
    </Helmet>

    <h1>{title}</h1>
    <p>Avro schemas are often designed with future evolution in mind: When a schema is updated, it’s generally preferable that consumers should be able to continue handling messages. One high-level goal of Chr.Avro is to facilitate flexible and predictable schema update processes.</p>

    <h2>Schema Registry compatibility levels</h2>
    <p>Confluent’s <ExternalLink to='https://github.com/confluentinc/schema-registry'>Schema Registry</ExternalLink> enforces <ExternalLink to='https://docs.confluent.io/current/schema-registry/docs/avro.html'>compatibility checks</ExternalLink> for schemas that it manages. Schema owners can choose one of these compatibility types:</p>
    <ul>
      <li><code>NONE</code>: No compatibility guaranteed.</li>
      <li><code>BACKWARD</code> and <code>BACKWARD_TRANSITIVE</code>: A reader using a newer schema version must be able to read data written using older schema versions.</li>
      <li><code>FORWARD</code> and <code>FORWARD_TRANSITIVE</code>: A reader using an older schema version must be able to read data written using newer schema versions.</li>
      <li><code>FULL</code> and <code>FULL_TRANSITIVE</code>: Backward and forward compatibility is guaranteed.</li>
    </ul>
    <p>If a subject is configured for <code>BACKWARD</code> compatibility (the default), each new version can only make backward-compatible changes. These compatibility types are only used by the registry, not by Chr.Avro or other Avro libraries. (Chr.Avro only concerns itself with whether the <DotnetReference id='T:Chr.Avro.Serialization.BinarySerializerBuilder'>serializer builder</DotnetReference> and <DotnetReference id='T:Chr.Avro.Serialization.BinaryDeserializerBuilder'>deserializer builder</DotnetReference> can <Link to='/internals/mapping'>map a schema to a .NET type</Link>.)</p>

    <h2>Producer and consumer implementations</h2>
    <p>When building producers and consumers with Chr.Avro.Confluent, there are two ways to configure Avro serialization and deserialization:</p>
    <ul>
      <li><strong>On the fly (async):</strong> Schemas are retrieved from the Schema Registry as needed. When producing, the serializer will derive a subject name from the topic being produced to (e.g., <code>topic_name-key</code> or <code>topic_name-value</code>) and optionally register a matching schema. When consuming, the deserializer will look up the schema <ExternalLink to='https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format'>by ID</ExternalLink>. Serializers and deserializers are not bound to a specific schema.</li>
      <li><strong>Bound at startup (sync):</strong> Serializers and deserializers are created for a specific schema.</li>
    </ul>
    <p>Chr.Avro.Confluent provides some extension methods to configure the Confluent.Kafka <DotnetReference id='T:Confluent.Kafka.ProducerBuilder`2'>producer</DotnetReference> and <DotnetReference id='T:Confluent.Kafka.ConsumerBuilder`2'>consumer</DotnetReference> builders for Avro serialization. In the example below, a producer is created with serializers bound at startup:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.ConfluentProducer
{
    public class Person
    {
        public Guid Id { get; set; }

        public string Name { get; set; }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "broker1:9092,broker2:9092"
            };

            var registryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            var builder = new ProducerBuilder<Guid, Person>(producerConfig);

            using (var registry = new CachedSchemaRegistryClient(registryConfig))
            {
                await Task.WhenAll(
                    builder.SetAvroKeySerializer(registry, "person-key", registerAutomatically: true),
                    builder.SetAvroValueSerializer(registry, "person-value", registerAutomatically: true)
                );
            }

            using (var producer = builder.Build())
            {
                // produce
            }
        }
    }
}
`}</Highlight>
    <p>Binding at startup is generally good practice for producers. If a type mapping exception is thrown, it will be thrown before the producer is created and any messages are produced. For consumers, on the other hand, it’s generally better to resolve schemas on the fly—the deserializer will be able to handle updates to the schema as long as the type can be mapped. The following example demonstrates how to configure async deserializers:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.ConfluentConsumer
{
    public class Person
    {
        public Guid Id { get; set; }

        public string Name { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "broker1:9092,broker2:9092",
                GroupId = "example_group"
            };

            var registryConfig = new SchemaRegistryConfig
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            var builder = new ConsumerBuilder<Guid, Person>(consumerConfig);

            using (var registry = new CachedSchemaRegistryClient(registryConfig))
            {
                builder.SetAvroKeyDeserializer(registry);
                builder.SetAvroValueDeserializer(registry);

                using (var consumer = builder.Build())
                {
                    // consume
                }
            }
        }
    }
}
`}</Highlight>
  </>
