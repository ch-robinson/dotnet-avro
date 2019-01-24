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
    <Highlight language='shell'>{`$ dotnet add package Chr.Avro.Confluent --version 1.0.0-rc.0`}</Highlight>
    <p>From there, <DotnetReference id='T:Chr.Avro.Confluent.SchemaRegistryProducerBuilder' /> can be used to build producers based on schemas from a Schema Registry instance:</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
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
            var brokers = "broker1:9092,broker2:9092";
            var registry = "http://registry:8081";
            var topic = "example-topic";

            var builder = new SchemaRegistryProducerBuilder(brokers, registry);

            using (var producer = await builder.BuildProducer<string, ExampleValue>("example-key", "example-value"))
            {
                await producer.ProduceAsync(topic, new Message<string, ExampleValue>
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
    <p><DotnetReference id='T:Chr.Avro.Confluent.SchemaRegistryConsumerBuilder' /> works pretty much the same way, but you don’t need to specify schemas ahead of time. Schemas will be retrieved from the Schema Registry as messages are consumed.</p>
    <Highlight language='csharp'>{`using Chr.Avro.Confluent;
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
            var brokers = "broker1:9092,broker2:9092";
            var group = "example-group";
            var registry = "http://registry:8081";
            var topic = "example-topic";

            var builder = new SchemaRegistryConsumerBuilder(brokers, group, registry);

            using (var consumer = builder.BuildConsumer<string, ExampleValue>())
            {
                consumer.Subscribe(topic);

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
