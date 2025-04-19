namespace Chr.Avro.RecordTests;

using System.IO;
using Chr.Avro.Representation;
using Chr.Avro.Serialization;
using BinaryReader = Chr.Avro.Serialization.BinaryReader;
using BinaryWriter = Chr.Avro.Serialization.BinaryWriter;

public partial class RecordNewFeaturesTest
{
    [Fact]
    public void Record()
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

        var serialiser = AvroSerialiser.Create<Player>(schema);

        var player = new Player("Alice", 25);
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);

        Assert.Equal(player, deserialized);
    }

    [Fact]
    public void Bug1_RecordWithNewProperty()
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

        var serialiser = AvroSerialiser.Create<Player2>(schema);

        var player = new Player2("Alice", 25);
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);

        Assert.Equal(player, deserialized);
    }

    public record Player(string Name, int Age);

    public record Player2(string Name, int Age, double Score = 0);
}
