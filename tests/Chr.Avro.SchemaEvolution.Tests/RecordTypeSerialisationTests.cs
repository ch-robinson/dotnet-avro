namespace Chr.Avro.SerialisationTests;

public class RecordTypeSerialisationTests
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
    public void RecordWithNewProperty()
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

    [Fact]
    public void RecordWithNewPropertyRequiresDefaultValue()
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

        var error = Assert.Throws<UnsupportedTypeException>(() => AvroSerialiser.Create<Player3>(schema)).Message;
        Assert.Contains("doesn't have a default constructor, and no compatible constructor could be found", error);
    }

    public record Player(string Name, int Age);

    public record Player2(string Name, int Age, double Score = 0);

    public record Player3(string Name, int Age, double Score);
}
