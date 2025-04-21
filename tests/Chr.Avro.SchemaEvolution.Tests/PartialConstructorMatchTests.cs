namespace Chr.Avro.SerialisationTests;

public class PartialConstructorMatchTests
{
    [Fact]
    public void ClassWithFewerPropertiesThanFieldsCanBeUsedForDeserialisation()
    {
        var schema = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int", "default": 0}
                ]
            }
            """;

        // Initial version of the schema and DTO -> Ok
        var serialiser = AvroSerialiser.Create<PlayerWithDefaultCtor>(schema);
        var player = new PlayerWithDefaultCtor { Name = "Alice" };
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);
        Assert.Equivalent(player, deserialized);

        var serialiser2 = AvroSerialiser.Create<Player>(schema);
        var player2 = new Player("Alice");
        bytes = serialiser.Serialise(player);
        var deserialized2 = serialiser2.Deserialise(bytes);
        Assert.Equivalent(player2, deserialized2);
    }

    [Fact]
    public void ClassWithMatchingConstructorButAdditionalProperties()
    {
        var schema = """
            {
                "name": "MyRecord",
                "type": "record",
                "fields": [
                    {"name":"name", "type":"string"},
                    {"name":"age", "type":"int", "default": 0}
                ]
            }
            """;

        var serialiser = AvroSerialiser.Create<PlayerWithAge>(schema);
        var player = new PlayerWithAge("Alice") { Age = 25 };
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);
        Assert.Equivalent(player, deserialized);
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

    [Fact]
    public void AddingConstructorParameterWithDefaultReferenceValueFails_Record()
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
        var serialiser = AvroSerialiser.Create<Player2>(schema);
        var player = new Player2("Alice", 25);
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

    [Fact]
    public void DeserialiserChosesTheBestConstructorAvailable()
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

        var serialiser = AvroSerialiser.Create<PlayerWithMultipleConstructors>(schema);
        var player = new PlayerWithMultipleConstructors("Alice", 25);
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);
        Assert.Equivalent(player, deserialized);
        Assert.Equal(2, deserialized.ConstructorUsed);
    }

    [Fact]
    public void DeserialiserHandlesCaseWhereNoConstructorMatch()
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

        Assert.Throws<UnsupportedTypeException>(() => AvroSerialiser.Create<PlayerWithUnmatchingConstructor>(schema));
    }

    [Fact]
    public void DeserialiserHandlesCaseWhereNoConstructorMatchExceptDefault()
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

        var serialiser = AvroSerialiser.Create<PlayerWithUnmatchingConstructorAndDefaultCtor>(schema);
        var player = new PlayerWithUnmatchingConstructorAndDefaultCtor("Alice", "New York");
        var bytes = serialiser.Serialise(player);
        var deserialized = serialiser.Deserialise(bytes);

        // Note: Address is not set in the deserialized object, because it's not in the schema
        var expected = new PlayerWithUnmatchingConstructorAndDefaultCtor("Alice", "Default");
        Assert.Equivalent(expected, deserialized);
    }

    public class Player
    {
        public Player(string name)
        {
            Name = name;
        }

        public string Name { get; }
    }

    public class PlayerWithMultipleConstructors
    {
        public PlayerWithMultipleConstructors(string name)
        {
            ConstructorUsed = 1;
            Name = name;
        }

        public PlayerWithMultipleConstructors(string name, int age)
        {
            ConstructorUsed = 2;
            Name = name;
            Age = age;
        }

        public PlayerWithMultipleConstructors(string name, string address)
        {
            ConstructorUsed = 3;
            Name = name;
            Address = address;
            Age = 20;
        }

        public PlayerWithMultipleConstructors(string name, int age, string address)
        {
            ConstructorUsed = 4;
            Name = name;
            Address = address;
            Age = age;
        }

        public int ConstructorUsed { get; }

        public string Name { get; }

        public string? Address { get; }

        public int Age { get; }
    }

    public class PlayerWithUnmatchingConstructor
    {
        public PlayerWithUnmatchingConstructor(string name, string address)
        {
            Name = name;
            Address = address;
            Age = 20;
        }

        public string Name { get; private set; }

        public string Address { get; private set; }

        public int Age { get; private set; }
    }

    public class PlayerWithUnmatchingConstructorAndDefaultCtor
    {
        public PlayerWithUnmatchingConstructorAndDefaultCtor()
        {
            Name = "Default";
            Address = "Default";
        }

        public PlayerWithUnmatchingConstructorAndDefaultCtor(string name, string address)
        {
            Name = name;
            Address = address;
            Age = 20;
        }

        public string Name { get; private set; }

        public string Address { get; private set; }

        public int Age { get; private set; }
    }

    public class PlayerWithAge
    {
        public PlayerWithAge(string name)
        {
            Name = name;
        }

        public string Name { get; }

        public int Age { get; set; }
    }

    public class PlayerWithDefaultCtor
    {
        public string? Name { get; set; }
    }

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
