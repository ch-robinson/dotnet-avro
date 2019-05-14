namespace Chr.Avro.Benchmarks
{
    using global::System.Collections.Generic;

    public static class IntSuite
    {
        public const int Iterations = 10_000_000;

        public const string Name = "int, 10e7 iterations";

        public const string Schema = "\"int\"";

        public static IEnumerable<int> Values => new[] { int.MinValue, -10, 10, int.MaxValue };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    public class IntRunner : Runner<int>
    {
        public IntRunner() : base(
            IntSuite.Name,
            IntSuite.Iterations,
            IntSuite.Schema,
            IntSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class IntRunner : Runner<int>
    {
        public IntRunner() : base(
            IntSuite.Name,
            IntSuite.Iterations,
            IntSuite.Schema,
            IntSuite.Values
        ) { }
    }
}
