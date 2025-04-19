namespace Chr.Avro.RecordTests;

public class Bug1
{
    [Fact]
    public void AddingConstructorParameterWithDefaultNullableValueFails_Record()
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

        // Initial version of the schema and DTO -> Ok
        var serialiser = AvroSerialiser.Create<Player>(schema);
        var player = new Player("Alice", 25);
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);
        Assert.Equal(player, deserialized);

        // Adding a double parameter with default value 0 -> Ok
        var serialiser2 = AvroSerialiser.Create<Player2>(schema);
        var player2 = new Player2("Alice", 25);
        bytes = serialiser2.Serialise(player2);
        var deserialized2 = serialiser2.Deserialise(bytes);
        Assert.Equal(player2, deserialized2);

        // Adding a double? parameter with default value null -> Fail
        var serialiser3 = AvroSerialiser.Create<Player3>(schema);
        var player3 = new Player3("Alice", 25);
        bytes = serialiser3.Serialise(player3);
        var deserialized3 = serialiser3.Deserialise(bytes);
        Assert.Equal(player3, deserialized3);
    }

    [Fact]
    public void AddingConstructorParameterWithDefaultNullableValueFails_Class()
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

        // Initial version of the schema and DTO -> Ok
        var serialiser = AvroSerialiser.Create<PlayerClass>(schema);
        var player = new PlayerClass("Alice", 25);
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);
        Assert.Equivalent(player, deserialized);

        // Adding a double parameter with default value 0 -> Ok
        var serialiser2 = AvroSerialiser.Create<PlayerClass2>(schema);
        var player2 = new PlayerClass2("Alice", 25);
        bytes = serialiser2.Serialise(player2);
        var deserialized2 = serialiser2.Deserialise(bytes);
        Assert.Equivalent(player2, deserialized2);

        // Adding a double? parameter with default value null -> Fail
        var serialiser3 = AvroSerialiser.Create<PlayerClass3>(schema);
        var player3 = new PlayerClass3("Alice", 25);
        bytes = serialiser3.Serialise(player3);
        var deserialized3 = serialiser3.Deserialise(bytes);
        Assert.Equivalent(player3, deserialized3);
    }

    [Fact]
    public void AddingConstructorParameterWithDefaultReferenceValueFails_Class()
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

        // Initial version of the schema and DTO -> Ok
        var serialiser = AvroSerialiser.Create<PlayerClass>(schema);
        var player = new PlayerClass("Alice", 25);
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);
        Assert.Equivalent(player, deserialized);

        // Adding an Address parameter with default value null -> Fail
        var serialiser2 = AvroSerialiser.Create<PlayerClass4>(schema);
        var player4 = new PlayerClass4("Alice", 25);
        bytes = serialiser2.Serialise(player4);
        var deserialized4 = serialiser2.Deserialise(bytes);
        Assert.Equivalent(player4, deserialized4);
    }

    public record Player(string Name, int Age);

    public record Player2(string Name, int Age, double Score = 0);

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

    public class PlayerClass2
    {
        public PlayerClass2(string name, int age, double score = 0)
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
