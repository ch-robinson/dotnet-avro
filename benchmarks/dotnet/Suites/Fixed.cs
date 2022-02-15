namespace Chr.Avro.Benchmarks
{
    using global::System;
    using global::System.Collections.Generic;

    public static class SmallFixedSuite
    {
        public const int Iterations = 1_000_000;

        public const string Name = "fixed[12]";

        public const string Schema = "{\"type\":\"fixed\",\"name\":\"Twelve\",\"size\":12}";

        public static IEnumerable<byte[]> Values => new[]
        {
            new byte[12]
        };
    }

    public static class LargeFixedSuite
    {
        public const int Iterations = 100_000;

        public const string Name = "fixed[2048]";

        public const string Schema = "{\"type\":\"fixed\",\"name\":\"TwentyFortyEight\",\"size\":2048}";

        public static IEnumerable<byte[]> Values => new[]
        {
            new byte[2048]
        };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    using global::Avro;
    using global::Avro.Generic;
    using global::Avro.Specific;
    using global::System.Linq;

    public class GenericSmallFixedRunner : GenericRunner<GenericFixed>
    {
        public GenericSmallFixedRunner() : base(
            $"{SmallFixedSuite.Name} (generic)",
            SmallFixedSuite.Iterations,
            SmallFixedSuite.Schema,
            SmallFixedSuite.Values.Select(value =>
                new GenericFixed((FixedSchema)Schema.Parse(SmallFixedSuite.Schema), value))
        ) { }
    }

    public class SpecificSmallFixedRunner : SpecificRunner<Twelve>
    {
        public SpecificSmallFixedRunner() : base(
            $"{SmallFixedSuite.Name} (specific)",
            SmallFixedSuite.Iterations,
            SmallFixedSuite.Schema,
            SmallFixedSuite.Values.Select(value => new Twelve()
            {
                Value = value
            })
        ) { }
    }

    public class GenericLargeFixedRunner : GenericRunner<GenericFixed>
    {
        public GenericLargeFixedRunner() : base(
            $"{LargeFixedSuite.Name} (generic)",
            LargeFixedSuite.Iterations,
            LargeFixedSuite.Schema,
            LargeFixedSuite.Values.Select(value =>
                new GenericFixed((FixedSchema)Schema.Parse(LargeFixedSuite.Schema), value))
        ) { }
    }

    public class SpecificLargeFixedRunner : SpecificRunner<TwentyFortyEight>
    {
        public SpecificLargeFixedRunner() : base(
            $"{LargeFixedSuite.Name} (specific)",
            LargeFixedSuite.Iterations,
            LargeFixedSuite.Schema,
            LargeFixedSuite.Values.Select(value => new TwentyFortyEight()
            {
                Value = value
            })
        ) { }
    }

    public partial class Twelve : SpecificFixed
    {
        public static Schema _SCHEMA = Schema.Parse("{\"type\":\"fixed\",\"name\":\"Twelve\",\"size\":12}");
        private static uint fixedSize = 12;
        public Twelve() : base(fixedSize)
        { }
        public override Schema Schema
        {
            get
            {
                return Twelve._SCHEMA;
            }
        }
        public static uint FixedSize
        {
            get
            {
                return Twelve.fixedSize;
            }
        }
    }

    public partial class TwentyFortyEight : SpecificFixed
    {
        public static Schema _SCHEMA = Schema.Parse("{\"type\":\"fixed\",\"name\":\"TwentyFortyEight\",\"size\":2048}");
        private static uint fixedSize = 2048;
        public TwentyFortyEight() : base(fixedSize)
        { }
        public override Schema Schema
        {
            get
            {
                return TwentyFortyEight._SCHEMA;
            }
        }
        public static uint FixedSize
        {
            get
            {
                return TwentyFortyEight.fixedSize;
            }
        }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class SmallFixedRunner : Runner<byte[]>
    {
        public SmallFixedRunner() : base(
            SmallFixedSuite.Name,
            SmallFixedSuite.Iterations,
            SmallFixedSuite.Schema,
            SmallFixedSuite.Values
        ) { }
    }

    public class LargeFixedRunner : Runner<byte[]>
    {
        public LargeFixedRunner() : base(
            LargeFixedSuite.Name,
            LargeFixedSuite.Iterations,
            LargeFixedSuite.Schema,
            LargeFixedSuite.Values
        ) { }
    }
}
