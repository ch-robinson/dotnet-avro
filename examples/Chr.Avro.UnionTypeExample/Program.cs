using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Chr.Avro.Abstract;
using Chr.Avro.Confluent;
using Chr.Avro.Representation;
using Chr.Avro.Resolution;
using Chr.Avro.UnionTypeExample.Cases;
using Chr.Avro.UnionTypeExample.Models;
using Chr.Avro.Serialization;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;

namespace Chr.Avro.UnionTypeExample
{
    internal class Program
    {
        private const string BootstrapServer = "localhost:9092";
        private const string RegistryServer = "http://localhost:8081";
        private const string Topic = "my-data";
        private const string SchemaName = "my-data-value";

        private static CachedSchemaRegistryClient _registryClient;

        private static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            _registryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = RegistryServer
            });

            GenerateSchema(true);
            Publish().Wait();
            Consume();
        }

        private static void GenerateSchema(bool register)
        {
            var cases = SchemaBuilder.CreateCaseBuilders(TemporalBehavior.Iso8601)
                .ToList()
                .Prepend(c => new MyBuilderCases(c));

            var schemaBuilder = new SchemaBuilder(cases);
            var schema = schemaBuilder.BuildSchema<MyMessage>();

            var writer = new JsonSchemaWriter();
            var json = writer.Write(schema);
            Console.Write(json);

            if (register)
                _registryClient.RegisterSchemaAsync(SchemaName, json).Wait();

        }

        private static async Task<DeliveryResult<Null, MyMessage>> Publish()
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = BootstrapServer
            };

            var codec = new BinaryCodec();
            var resolver = new ReflectionResolver();

            var binarySerializerBuilder = new BinarySerializerBuilder(BinarySerializerBuilder
                .CreateBinarySerializerCaseBuilders(codec)
                .Prepend(builder => new DataSerializerCase(resolver, codec, builder)), resolver);

            var registrySerializerBuilder = new SchemaRegistrySerializerBuilder(_registryClient, serializerBuilder: binarySerializerBuilder);

            var producer = new ProducerBuilder<Null, MyMessage>(producerConfig)
                .SetValueSerializer(await registrySerializerBuilder.Build<MyMessage>(SchemaName))
                .Build();

            return await producer.ProduceAsync(Topic, new Message<Null, MyMessage>
            {
                Value = new MyMessage
                {
                    Name = "TestName",
                    DateTime = DateTime.Now,
                    Payload = new Dictionary<string, IDataObj>
                    {
                        {
                            "Obj1", new DataObj1
                            {
                                Name = "Obj1",
                                Value = 123
                            }
                        },
                        {
                            "Obj2", new DataObj2
                            {
                                Name = "Obj2",
                                Value = true,
                                Foo = 456
                            }
                        },
                        {
                            "Obj3", new DataObj3
                            {
                                Name = "Obj3",
                                Value = 7.89
                            }
                        }
                    }
                }
            });
        }

        private static void Consume()
        {
            var consumerConfig = new ConsumerConfig()
            {
                BootstrapServers = BootstrapServer,
                GroupId = Dns.GetHostName(),
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var codec = new BinaryCodec();
            var resolver = new ReflectionResolver(); // customize as needed

            var deserializerBuilder = new BinaryDeserializerBuilder(BinaryDeserializerBuilder.CreateBinaryDeserializerCaseBuilders(codec)
                .Prepend(builder => new DataDeserializerCase(resolver, codec, builder)), resolver);


            var consumer = new ConsumerBuilder<Ignore, MyMessage>(consumerConfig)
                .SetValueDeserializer(new AsyncSchemaRegistryDeserializer<MyMessage>(_registryClient, deserializerBuilder).AsSyncOverAsync())
                .Build();

            consumer.Subscribe(Topic);
            while (true)
            {
                var result = consumer.Consume();
                Console.WriteLine($"{result.Message.Value.Name} {result.Message.Value.DateTime}");
            }
        }
    }
}
