namespace Chr.Avro.Benchmarks
{
    using global::System.Collections.Generic;

    public static class StringSuite
    {
        public const int Iterations = 10_000_000;

        public const string Name = "string, 10e7 iterations";

        public const string Schema = "\"string\"";

        public static IEnumerable<string> Values => new[] { string.Empty, "🐯", "tiger", "Tyger Tyger, burning bright,\nIn the forests of the night;\nWhat immortal hand or eye,\nCould frame thy fearful symmetry?" };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    public class StringRunner : GenericRunner<string>
    {
        public StringRunner() : base(
            StringSuite.Name,
            StringSuite.Iterations,
            StringSuite.Schema,
            StringSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class StringRunner : Runner<string>
    {
        public StringRunner() : base(
            StringSuite.Name,
            StringSuite.Iterations,
            StringSuite.Schema,
            StringSuite.Values
        ) { }
    }
}
