using Chr.Avro.Abstract;
using Chr.Avro.Example.Models;
using Chr.Avro.Resolution;
using Chr.Avro.Serialization;
using System;
using System.Collections.Generic;

namespace Chr.Avro.Example
{
    class Program
    {
        static void Main(string[] args)
        {
            var schema = new SchemaBuilder().BuildSchema<ExampleClass>();
            var serializer = new BinarySerializerBuilder().BuildSerializer<ExampleClass>(schema);

            var exampleClass = new ExampleClass() { ExpectedDate = DateTime.Now.AddDays(-100), Id = 12345, Items = new List<ExampleItem>() { new ExampleItem() { Description = "Object 1", Id = 99 }, new ExampleItem() { Description = "Object 2", Id = 100 } } };

            var serialized = serializer.Serialize(exampleClass);

            var deserializer = new BinaryDeserializerBuilder(resolver: new ReflectionResolver()).BuildDeserializer<ExampleMappedClass>(schema);

            var deserialized = deserializer.Deserialize(serialized);
        }
    }
}
