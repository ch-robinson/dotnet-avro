namespace Chr.Avro.Benchmarks
{
    using global::System;
    using global::System.Collections.Generic;

    public static class RecordSuite
    {
        public const int Iterations = 100_000;

        public const string Name = "record";

        public const string Schema = "{\"type\":\"record\",\"name\":\"Person\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"birthdate\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"favoriteColor\",\"type\":{\"type\":\"enum\",\"name\":\"Color\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}}]}";

        public static IEnumerable<Person> Values => new[]
        {
            new Person()
            {
                Name = "Albert Fountain",
                Birthdate = new DateTime(1972, 2, 27),
                FavoriteColor = Color.Blue
            },
            new Person()
            {
                Name = "Doris Hill",
                Birthdate = new DateTime(1982, 6, 12),
                FavoriteColor = Color.Red
            }
        };

        public enum Color
        {
            Blue,
            Green,
            Red
        }

        public class Person
        {
            public string Name { get; set; }

            public DateTime Birthdate { get; set; }

            public Color FavoriteColor { get; set; }
        }
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    using global::Avro;
    using global::Avro.Generic;
    using global::Avro.Specific;
    using global::System;
    using global::System.Linq;

    public class GenericRecordRunner : GenericRunner<GenericRecord>
    {
        public GenericRecordRunner() : base(
            $"{RecordSuite.Name} (generic)",
            RecordSuite.Iterations,
            RecordSuite.Schema,
            RecordSuite.Values.Select(value =>
            {
                var person = (RecordSchema)Schema.Parse(RecordSuite.Schema);
                var color = (EnumSchema)person["favoriteColor"].Schema;

                var record = new GenericRecord(person);

                record.Add("name", value.Name);
                record.Add("birthdate", (long)value.Birthdate.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds);
                record.Add("favoriteColor", new GenericEnum(color, value.FavoriteColor.ToString().ToUpperInvariant()));

                return record;
            })
        ) { }
    }

    public class SpecificRecordRunner : SpecificRunner<Person>
    {
        public SpecificRecordRunner() : base(
            $"{RecordSuite.Name} (specific)",
            RecordSuite.Iterations,
            RecordSuite.Schema,
            RecordSuite.Values.Select(value => new Person()
            {
                name = value.Name,
                birthdate = (long)value.Birthdate.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds,
                favoriteColor = (Color)Enum.Parse(typeof(Color), value.FavoriteColor.ToString().ToUpperInvariant())
            })
        ) { }
    }

    public enum Color
	{
		RED,
		GREEN,
		BLUE,
	}

    public class Person : ISpecificRecord
	{
		public static Schema _SCHEMA = Schema.Parse("{\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"chr\",\"fields\":[{\"name\":\"name\",\"type" +
				"\":\"string\"},{\"name\":\"birthdate\",\"type\":\"long\"},{\"name\":\"favoriteColor\",\"type\":{\"" +
				"type\":\"enum\",\"name\":\"Color\",\"namespace\":\"chr\",\"symbols\":[\"RED\",\"GREEN\",\"BLUE\"]}}" +
				"]}");
		private string _name;
		private long _birthdate;
		private Color _favoriteColor;
		public virtual Schema Schema
		{
			get
			{
				return Person._SCHEMA;
			}
		}
		public string name
		{
			get
			{
				return this._name;
			}
			set
			{
				this._name = value;
			}
		}
		public long birthdate
		{
			get
			{
				return this._birthdate;
			}
			set
			{
				this._birthdate = value;
			}
		}
		public Color favoriteColor
		{
			get
			{
				return this._favoriteColor;
			}
			set
			{
				this._favoriteColor = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
			case 0: return this.name;
			case 1: return this.birthdate;
			case 2: return this.favoriteColor;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
			case 0: this.name = (System.String)fieldValue; break;
			case 1: this.birthdate = (System.Int64)fieldValue; break;
			case 2: this.favoriteColor = (Color)fieldValue; break;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class RecordRunner : Runner<RecordSuite.Person>
    {
        public RecordRunner() : base(
            RecordSuite.Name,
            RecordSuite.Iterations,
            RecordSuite.Schema,
            RecordSuite.Values
        ) { }
    }
}
