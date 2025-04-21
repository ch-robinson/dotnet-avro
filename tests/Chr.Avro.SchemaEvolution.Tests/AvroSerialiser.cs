namespace Chr.Avro.SerialisationTests;

using Chr.Avro.Representation;
using Chr.Avro.Serialization;

public static class AvroSerialiser
{
    public static AvroSerialiser<T> Create<T>(string schema)
    {
        return new AvroSerialiser<T>(schema);
    }
}

#pragma warning disable SA1402 // File may only contain a single type
public class AvroSerialiser<T>
#pragma warning restore SA1402 // File may only contain a single type
{
    private static readonly JsonSchemaReader SchemaReader = new();
    private static readonly BinaryDeserializerBuilder DeserializerBuilder = new();
    private static readonly BinarySerializerBuilder SerializerBuilder = new();

    private readonly BinaryDeserializer<T> deserialize;
    private readonly BinarySerializer<T> serialize;

    public AvroSerialiser(string schema)
    {
        var avroSchema = SchemaReader.Read(schema);
        deserialize = DeserializerBuilder.BuildDelegate<T>(avroSchema);
        serialize = SerializerBuilder.BuildDelegate<T>(avroSchema);
    }

    public byte[] Serialise(T value)
    {
        using var stream = new MemoryStream();
        serialize(value, new BinaryWriter(stream));
        return stream.ToArray();
    }

    public T Deserialise(byte[] bytes)
    {
        var reader = new BinaryReader(bytes);
        return deserialize(ref reader);
    }
}
