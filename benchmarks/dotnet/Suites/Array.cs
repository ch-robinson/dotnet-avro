namespace Chr.Avro.Benchmarks
{
    using global::System.Collections.Generic;

    public static class DoubleArraySuite
    {
        public const int Iterations = 100_000;

        public const string Name = "array[double]";

        public const string Schema = "{\"type\":\"array\",\"items\":\"double\"}";

        public static IEnumerable<List<double>> Values => new[]
        {
            new List<double> { 1, 2, 4, 8, 16, 32, 64, 128, 256, 512 },
            new List<double> { 44.822560, -93.464530 }
        };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    using global::System.Collections.Generic;
    using global::System.Linq;

    public class GenericDoubleArrayRunner : GenericRunner<object[]>
    {
        public GenericDoubleArrayRunner() : base(
            $"{DoubleArraySuite.Name} (generic)",
            DoubleArraySuite.Iterations,
            DoubleArraySuite.Schema,
            DoubleArraySuite.Values.Select(value => value.Select(item => (object)item).ToArray())
        ) { }
    }

    public class SpecificDoubleArrayRunner : SpecificRunner<List<double>>
    {
        public SpecificDoubleArrayRunner() : base(
            $"{DoubleArraySuite.Name} (specific)",
            DoubleArraySuite.Iterations,
            DoubleArraySuite.Schema,
            DoubleArraySuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    using global::System.Collections.Generic;

    public class DoubleArrayRunner : Runner<List<double>>
    {
        public DoubleArrayRunner() : base(
            DoubleArraySuite.Name,
            DoubleArraySuite.Iterations,
            DoubleArraySuite.Schema,
            DoubleArraySuite.Values
        ) { }
    }
}
