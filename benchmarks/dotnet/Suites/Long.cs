namespace Chr.Avro.Benchmarks
{
    using global::System.Collections.Generic;

    public static class LongSuite
    {
        public const int Iterations = 10_000_000;

        public const string Name = "long, 10e7 iterations";

        public const string Schema = "\"long\"";

        public static IEnumerable<long> Values => new[] { long.MinValue, -20, 20, long.MaxValue };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    public class LongRunner : Runner<long>
    {
        public LongRunner() : base(
            LongSuite.Name,
            LongSuite.Iterations,
            LongSuite.Schema,
            LongSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class LongRunner : Runner<long>
    {
        public LongRunner() : base(
            LongSuite.Name,
            LongSuite.Iterations,
            LongSuite.Schema,
            LongSuite.Values
        ) { }
    }
}
