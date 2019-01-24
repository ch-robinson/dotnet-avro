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
      <li><code>BACKWARD</code> and <code>BACKWARD_TRANSITIVE</code>: A reader using the latest schema must be able to read data written using previous schemas.</li>
      <li><code>FORWARD</code> and <code>FORWARD_TRANSITIVE</code>: A reader using previous schemas must be able to read data written using the latest schema.</li>
      <li><code>FULL</code> and <code>FULL_TRANSITIVE</code>: Backward and forward compatibility is guaranteed.</li>
    </ul>
    <p>If a subject is configured for <code>BACKWARD</code> compatibility (the default), each new version can only make backward-compatible changes. These compatibility types are only used by the registry, not by Chr.Avro or other Avro libraries.</p>

    <h2>Producer and consumer implementations</h2>
    <p>Chr.Avro only concerns itself with <Link to='/internals/mapping'>whether a schema can be mapped to a .NET type</Link>. The <DotnetReference id='T:Chr.Avro.Serialization.BinarySerializerBuilder'>serializer builder</DotnetReference> and <DotnetReference id='T:Chr.Avro.Serialization.BinaryDeserializerBuilder'>deserializer builder</DotnetReference> make that determination when they build delegates, throwing if no mapping exists.</p>
    <p>The builders in the Chr.Avro.Confluent library all assume the <ExternalLink to='https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format'>Confluent wire format</ExternalLink>, which prepends a schema ID to the serialized Avro data. To construct a producer, the <DotnetReference id='T:Chr.Avro.Confluent.SchemaRegistryProducerBuilder'>producer builder</DotnetReference> retrieves a schema from the provided Schema Registry and then attempts to build a matching serializer:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.ConfluentProducer
{
    public class ClassWithProperty
    {
        public string Property { get; set; }
    }

    public class ClassWithoutProperty
    {

    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var brokers = "broker1:9092,broker2:9092";
            var registry = "http://registry:8081";

            var builder = new SchemaRegistryProducerBuilder(brokers, registry);

            // $ curl http://registry:8081/subjects/example-key/versions/1
            // "string"
            // $ curl http://registry:8081/subjects/example-value/versions/1
            // {"name":"example.Class",type":"record","fields":[{"name":"property","type":"string"}]}

            // succeeds:
            await builder.BuildProducer<string, ClassWithProperty>("example-key", 1, "example-value", 1);

            // throws:
            await builder.BuildProducer<string, ClassWithoutProperty>("example-key", 1, "example-value", 1);
        }
    }
}
`}</Highlight>
    <p>The <DotnetReference id='T:Chr.Avro.Confluent.SchemaRegistryConsumerBuilder'>consumer builder</DotnetReference> provides a similar interface. That behavior isn’t always desirable, though, because consumers will be tied to a specific schema version. For that reason, the consumer builder can also build lazy-resolving consumers:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.ConfluentProducer
{
    public class ExampleClass
    {
        public string Property { get; set; }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var brokers = "broker1:9092,broker2:9092";
            var group = "example-group";
            var registry = "http://registry:8081";

            var builder = new SchemaRegistryConsumerBuilder(brokers, group, registry);

            // builds a consumer for a specific schema version:
            await builder.BuildConsumer<string, ExampleClass>("example-key", 1, "example-value", 1);

            // builds a consumer that fetches schemas lazily:
            builder.BuildConsumer<string, ExampleClass>();
        }
    }
}
`}</Highlight>
    <p>The lazy-resolving consumer behaves similarly to the <DotnetReference id='T:Confluent.Kafka.Serialization.AvroDeserializer`1[]'>Avro deserializer</DotnetReference> in Confluent’s Kafka library.</p>
  </>
