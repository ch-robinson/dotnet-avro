namespace Chr.Avro.Benchmarks
{
    using global::System.Collections.Generic;

    public static class BooleanSuite
    {
        public const int Iterations = 10_000_000;

        public const string Name = "boolean";

        public const string Schema = "\"boolean\"";

        public static IEnumerable<bool> Values => new[] { true, false };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    public class BooleanRunner : GenericRunner<bool>
    {
        public BooleanRunner() : base(
            BooleanSuite.Name,
            BooleanSuite.Iterations,
            BooleanSuite.Schema,
            BooleanSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class BooleanRunner : Runner<bool>
    {
        public BooleanRunner() : base(
            BooleanSuite.Name,
            BooleanSuite.Iterations,
            BooleanSuite.Schema,
            BooleanSuite.Values
        ) { }
    }
}
