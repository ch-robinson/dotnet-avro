import React from 'react'
import { Helmet } from 'react-helmet'

import Highlight from '../../components/code/highlight'
import DotnetReference from '../../components/references/dotnet'
import ExternalLink from '../../components/site/external-link'

const title = 'Building Kafka producers and consumers'

export default () =>
  <>
    <Helmet>
      <title>{title}</title>
    </Helmet>

    <h1>{title}</h1>
    <p>If you’re using Avro, there’s a good chance you’re using it to serialize and deserialize Kafka messages. Chr.Avro ships with first-class support for <ExternalLink to='https://github.com/confluentinc/confluent-kafka-dotnet'>Confluent’s Kafka clients</ExternalLink>, but also supports building serializers and deserializers to use elsewhere.</p>

    <h2>Using the producer and consumer builders</h2>
    <p>Chr.Avro’s producer and consumer builders are the shortest path to working Kafka clients. To use the builders, first add a reference to the Chr.Avro.Confluent package:</p>
    <Highlight language='shell'>{`$ dotnet add package Chr.Avro.Confluent --version 1.0.0-rc.5`}</Highlight>
    <p>From there, <DotnetReference id='T:Chr.Avro.Confluent.SchemaRegistryProducerBuilder`2' /> can be used to build producers based on schemas from a Schema Registry instance:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using System;
using System.Collections.Generic;
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
            var builder = new SchemaRegistryProducerBuilder<string, ExampleValue>(new Dictionary<string, string>
            {
                { "bootstrap.servers", "broker1:9092,broker2:9092" },
                { "schema.registry.url", "http://registry:8081" }
            });

            using (var producer = builder.Build())
            {
                await producer.ProduceAsync("example_topic", new Message<string, ExampleValue>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = new ExampleValue
                    {
                        Property = "example!"
                    }
                });
            }
        }
    }
}`}</Highlight>
    <p>The producer builder assumes (per Confluent convention) that the key and value subjects for <code>example_topic</code> are <code>example_topic-key</code> and <code>example_topic-value</code>. When messages are published, the key and value serializers will attempt to pull down schemas from the Schema Registry.</p>
    <p><DotnetReference id='T:Chr.Avro.Confluent.SchemaRegistryConsumerBuilder`2' /> works in a similar way—schemas will be retrieved from the Schema Registry as messages are consumed.</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
using System;
using System.Collections.Generic;

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
            var builder = new SchemaRegistryConsumerBuilder<string, ExampleValue>(new Dictionary<string, string>()
            {
                { "bootstrap.servers", "broker1:9092,broker2:9092" },
                { "group.id", "example_consumer_group" },
                { "schema.registry.url", "http://registry:8081" }
            });

            using (var consumer = builder.BuildConsumer<string, ExampleValue>())
            {
                consumer.Subscribe("example_topic");

                while (true)
                {
                    var result = consumer.Consume();
                    Console.WriteLine($"{result.Key}: {result.Value.Property}");
                }

                consumer.Close();
            }
        }
    }
}`}</Highlight>
  </>
