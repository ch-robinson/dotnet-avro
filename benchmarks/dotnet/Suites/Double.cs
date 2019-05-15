namespace Chr.Avro.Benchmarks
{
    using global::System;
    using global::System.Collections.Generic;

    public static class DoubleSuite
    {
        public const int Iterations = 10_000_000;

        public const string Name = "double";

        public const string Schema = "\"double\"";

        public static IEnumerable<double> Values => new[] { double.MinValue, -Math.E, Math.PI, double.MaxValue };
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    public class DoubleRunner : GenericRunner<double>
    {
        public DoubleRunner() : base(
            DoubleSuite.Name,
            DoubleSuite.Iterations,
            DoubleSuite.Schema,
            DoubleSuite.Values
        ) { }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    public class DoubleRunner : Runner<double>
    {
        public DoubleRunner() : base(
            DoubleSuite.Name,
            DoubleSuite.Iterations,
            DoubleSuite.Schema,
            DoubleSuite.Values
        ) { }
    }
}
