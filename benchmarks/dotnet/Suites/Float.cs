namespace Chr.Avro.Benchmarks
{
    using global::System;
    using global::System.Collections.Generic;

    public static class FloatSuite
    {
        public const int Iterations = 1_000_000;

        public const string Name = "float";

        public const string Schema = "\"float\"";

        public static IEnumerable<float> Values => new[] { float.MinValue, (float)(-Math.E), (float)Math.PI, float.MaxValue };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    public class FloatRunner : GenericRunner<float>
    {
        public FloatRunner() : base(
            FloatSuite.Name,
            FloatSuite.Iterations,
            FloatSuite.Schema,
            FloatSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class FloatRunner : Runner<float>
    {
        public FloatRunner() : base(
            FloatSuite.Name,
            FloatSuite.Iterations,
            FloatSuite.Schema,
            FloatSuite.Values
        ) { }
    }
}
