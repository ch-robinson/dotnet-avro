using Chr.Avro.Representation;
using Chr.Avro.Serialization;

namespace Chr.Avro.SerialisationTests;

public class NewRecordFieldTests
{
    private static readonly JsonSchemaReader SchemaReader = new();
    private static readonly BinaryDeserializerBuilder DeserializerBuilder = new();
    private static readonly BinarySerializerBuilder SerializerBuilder = new();

    [Fact]
    public void SchemaEvolution_NewFieldWithDefaultValue()
    {
        var schema = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int"}
                ]
            }
            """;

        var schema2 = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int"},
                    {"name":"score", "type":"double", "default":3.14}
                ]
            }
            """;

        // Initial version of the schema and DTO -> Ok
        var serialiser11 = AvroSerialiser.Create<Player>(schema);
        var player = new Player("Alice", 25);
        var bytes = serialiser11.Serialise(player);
        var deserialized = serialiser11.Deserialise(bytes);
        Assert.Equal(player, deserialized);

        // New schema and DTO with default value -> Ok
        var serialiser22 = AvroSerialiser.Create<Player2>(schema2);
        var player2 = new Player2("Alice", 25);
        bytes = serialiser22.Serialise(player2);
        var deserialized2 = serialiser22.Deserialise(bytes);
        Assert.Equal(player, deserialized);

        // Adding a double field to schema with default value 0 -> Fail when used with v1 DTO
        var serialiser12 = AvroSerialiser.Create<Player>(schema2);
        bytes = serialiser12.Serialise(player);
        deserialized = serialiser12.Deserialise(bytes);
        Assert.Equal(new Player2("Alice", 25), deserialized2);

        // We should also be able to deserialize to v2 DTO
        deserialized2 = serialiser22.Deserialise(bytes);
        Assert.Equal(new Player2("Alice", 25), deserialized2);
    }

    [Fact]
    public void SchemaEvolution_NewFieldWithDefaultNullValue()
    {
        var schema = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int"}
                ]
            }
            """;

        var schema3 = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int"},
                    {"name":"score", "type": ["null", "double"], "default":null}
                ]
            }
            """;

        // Initial version of the schema and DTO -> Ok
        var serialiser11 = AvroSerialiser.Create<Player>(schema);
        var player = new Player("Alice", 25);
        var bytes = serialiser11.Serialise(player);
        var deserialized = serialiser11.Deserialise(bytes);
        Assert.Equal(player, deserialized);

        // New schema and DTO with default value -> Ok
        var serialiser33 = AvroSerialiser.Create<Player3>(schema3);
        var player3 = new Player3("Alice", 25);
        bytes = serialiser33.Serialise(player3);
        var deserialized3 = serialiser33.Deserialise(bytes);
        Assert.Equal(player3, deserialized3);

        // Adding a double field to schema with default value null -> Fail when used with v1 DTO
        var serialiser13 = AvroSerialiser.Create<Player>(schema3);
        bytes = serialiser13.Serialise(player);
        deserialized = serialiser13.Deserialise(bytes);
        Assert.Equal(new Player("Alice", 25), deserialized);

        // We should also be able to deserialize to v3 DTO
        deserialized3 = serialiser33.Deserialise(bytes);
        Assert.Equal(new Player3("Alice", 25), deserialized3);
    }

    [Fact]
    public void SchemaEvolution_NewFieldWithoutDefaultValueCannotBeSerialized()
    {
        var schema = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int"},
                    {"name":"score", "type":"double"}
                ]
            }
            """;

        // New schema and DTO without default value -> Fail
        var avroSchema = SchemaReader.Read(schema);

        // Player is missing "score", and no default value exists in the schema
        var exception = Assert.Throws<UnsupportedTypeException>(() => SerializerBuilder.BuildDelegate<Player>(avroSchema));
        var typeName = typeof(Player).FullName;
        Assert.Equal($"{typeName} does not have a field or property that matches the score field on MyRecord.", exception.Message);
    }

    [Fact]
    public void InvalidFieldType()
    {
        var schema = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"string"}
                ]
            }
            """;

        var avroSchema = SchemaReader.Read(schema);

        // Player field 'age' has incorrect type
        var exception = Assert.Throws<UnsupportedTypeException>(() => SerializerBuilder.BuildDelegate<Player>(avroSchema));
        var typeName = typeof(Player).FullName;
        Assert.Equal($"The Age member on {typeName} could not be mapped to the age field on MyRecord.", exception.Message);
    }

    public record Player(string Name, int Age);

    public record Player2(string Name, int Age, double Score = 3.14);

    public record Player3(string Name, int Age, double? Score = null);

    public class PlayerClass
    {
        public PlayerClass(string name, int age)
        {
            Name = name;
            Age = age;
        }

        public string Name { get; }

        public int Age { get; }
    }

    public class PlayerWithScore
    {
        public PlayerWithScore(string name, int age, double score = 0)
        {
            Name = name;
            Age = age;
            Score = score;
        }

        public string Name { get; }

        public int Age { get; }

        public double Score { get; }
    }

    public class PlayerClass3
    {
        public PlayerClass3(string name, int age, double? score = null)
        {
            Name = name;
            Age = age;
            Score = score ?? 0;
        }

        public string Name { get; }

        public int Age { get; }

        public double Score { get; }
    }

    public class PlayerClass4
    {
        public PlayerClass4(string name, int age, Address? addess = null)
        {
            Name = name;
            Age = age;
            Addess = addess;
        }

        public string Name { get; }

        public int Age { get; }

        public Address? Addess { get; }
    }

    public class Address
    {
        public string? Street { get; set; }

        public string? City { get; set; }
    }
}
