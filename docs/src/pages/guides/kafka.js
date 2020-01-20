import { graphql, useStaticQuery } from 'gatsby'
import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import DotnetReference from '../../components/references/dotnet'
import NugetPackageReference from '../../components/references/nuget-package'
import ExternalLink from '../../components/site/external-link'

const title = 'Building Kafka producers and consumers'

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
      <p>{projectName} ships with first-class support for <ExternalLink to='https://github.com/confluentinc/confluent-kafka-dotnet'>Confluent’s Kafka clients</ExternalLink>, the shortest path to creating Kafka producers and consumers in .NET.</p>

      <h2>Using Confluent’s client builders</h2>
      <p>First, add a reference to the Chr.Avro.Confluent package:</p>
      <Highlight language='bash'>{`$ dotnet add package Chr.Avro.Confluent --version ${latestRelease}`}</Highlight>
      <p>Chr.Avro.Confluent depends on <NugetPackageReference id='Confluent.Kafka' />, which contains <DotnetReference id='T:Confluent.Kafka.ProducerBuilder`2'>producer</DotnetReference> and <DotnetReference id='T:Confluent.Kafka.ConsumerBuilder`2'>consumer</DotnetReference> builders. To build a <ExternalLink to='https://www.confluent.io/confluent-schema-registry/'>Schema Registry</ExternalLink>-integrated producer, use the producer builder in tandem with {projectName}’s Avro extension methods:</p>
      <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Chr.Avro.Examples.KafkaProducer
{
    public class ExampleValue
    {
        public string Property { get; set; }
    }

    public class Program
    {
        public static async Task Main(string[] args)
        {
            var producerConfig = new ProducerConfig()
            {
                BootstrapServers = "broker1:9092,broker2:9092"
            };

            var registryConfig = new RegistryConfig()
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            using (var registry = new CachedSchemaRegistryClient(registryConfig))
            {
                var builder = new ProducerBuilder<Ignore, ExampleValue>(producerConfig)
                    .SetAvroValueSerializer(registry, registerAutomatically: false)
                    .SetErrorHandler((_, error) => Console.Error.WriteLine(error.ToString()));

                using (var producer = builder.Build())
                {
                    await producer.ProduceAsync("example_topic", new Message<Ignore, ExampleValue>
                    {
                        Value = new ExampleValue
                        {
                            Property = "example!"
                        }
                    });
                }
            }
        }
    }
}`}</Highlight>
    <p>The serializer assumes (per Confluent convention) that the value subject for <code>example_topic</code> is <code>example_topic-value</code>. (The key subject would be <code>example_topic-key</code>.) When messages are published, the serializer will attempt to pull down a schema from the Schema Registry. The serializer can be configured to generate and register a schema automatically if one doesn’t exist.</p>
    <p>Building consumers works in a similar way—schemas will be retrieved from the Schema Registry as messages are consumed:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;

namespace Chr.Avro.Examples.KafkaConsumer
{
    public class ExampleValue
    {
        public string Property { get; set; }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = "broker1:9092,broker2:9092",
                GroupId = "example_consumer_group"
            };

            var registryConfig = new RegistryConfig()
            {
                SchemaRegistryUrl = "http://registry:8081"
            };

            using (var registry = new CachedSchemaRegistryClient(registryClient))
            {
                var builder = new ConsumerBuilder<Ignore, ExampleValue>(consumerConfig)
                    .SetAvroValueDeserializer(registry)
                    .SetErrorHandler((_, error) => Console.Error.WriteLine(error.ToString()));

                using (var consumer = builder.Build())
                {
                    consumer.Subscribe("example_topic");

                    while (true)
                    {
                        var result = consumer.Consume();
                        Console.WriteLine(result.Value.Property);
                    }
                }
            }
        }
    }
}`}</Highlight>
    </>
  )
}
