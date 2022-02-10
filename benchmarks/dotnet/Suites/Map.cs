namespace Chr.Avro.Benchmarks
{
    using global::System;
    using global::System.Collections.Generic;

    public static class DoubleMapSuite
    {
        public const int Iterations = 100_000;

        public const string Name = "map[double]";

        public const string Schema = "{\"type\":\"map\",\"values\":\"double\"}";

        public static IEnumerable<Dictionary<string, double>> Values => new[]
        {
            new Dictionary<string, double>
            {
                { "e", Math.E },
                { "pi", Math.PI },
                { "tau", Math.PI * 2 }
            }
        };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    using global::System.Collections.Generic;
    using global::System.Linq;

    public class GenericDoubleMapRunner : GenericRunner<Dictionary<string, object>>
    {
        public GenericDoubleMapRunner() : base(
            $"{DoubleMapSuite.Name} (generic)",
            DoubleMapSuite.Iterations,
            DoubleMapSuite.Schema,
            DoubleMapSuite.Values.Select(value => value.ToDictionary(pair => pair.Key, pair => (object)pair.Value))
        ) { }
    }

    public class SpecificDoubleMapRunner : SpecificRunner<Dictionary<string, double>>
    {
        public SpecificDoubleMapRunner() : base(
            $"{DoubleMapSuite.Name} (specific)",
            DoubleMapSuite.Iterations,
            DoubleMapSuite.Schema,
            DoubleMapSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    using global::System.Collections.Generic;

    public class DoubleMapRunner : Runner<Dictionary<string, double>>
    {
        public DoubleMapRunner() : base(
            DoubleMapSuite.Name,
            DoubleMapSuite.Iterations,
            DoubleMapSuite.Schema,
            DoubleMapSuite.Values
        ) { }
    }
}
