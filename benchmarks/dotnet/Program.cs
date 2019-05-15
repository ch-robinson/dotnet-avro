using CsvHelper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace Chr.Avro.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var csv = new CsvWriter(Console.Out))
            {
                // primitives:
                /* csv.WriteRecords(Run<Apache.BooleanRunner>());
                csv.WriteRecords(Run<Chr.BooleanRunner>());
                csv.WriteRecords(Run<Apache.DoubleRunner>());
                csv.WriteRecords(Run<Chr.DoubleRunner>());
                csv.WriteRecords(Run<Apache.FloatRunner>());
                csv.WriteRecords(Run<Chr.FloatRunner>());
                csv.WriteRecords(Run<Apache.IntRunner>());
                csv.WriteRecords(Run<Chr.IntRunner>());
                csv.WriteRecords(Run<Apache.LongRunner>());
                csv.WriteRecords(Run<Chr.LongRunner>());
                csv.WriteRecords(Run<Apache.StringRunner>());
                csv.WriteRecords(Run<Chr.StringRunner>());*/

                // records:
                csv.WriteRecords(Run<Apache.GenericRecordRunner>());
                csv.WriteRecords(Run<Apache.SpecificRecordRunner>());
                csv.WriteRecords(Run<Chr.RecordRunner>());
            }
        }

        public static IEnumerable<Result> Run<T>(int iterations = 5) where T : IRunner, new()
        {
            var runner = new T();

            for (int i = 1; i <= iterations; i++)
            {
                foreach (var (component, time) in runner.Run())
                {
                    yield return new Result()
                    {
                        Runtime = ".NET Core 2.2",
                        Library = runner.Library,
                        Suite = runner.Suite,
                        Component = component,
                        Iteration = i,
                        Duration = time.TotalMilliseconds
                    };
                }
            }
        }
    }

    public sealed class Result
    {
        public string Runtime { get; set; }

        public string Library { get; set; }

        public string Suite { get; set; }

        public string Component { get; set; }

        public int Iteration { get; set; }

        public double Duration { get; set; }
    }

    public interface IRunner
    {
        string Library { get; }

        string Suite { get; }

        IEnumerable<(string, TimeSpan)> Run();
    }

    public abstract class Runner<T> : IRunner
    {
        public string Library { get; }

        public string Suite { get; }

        protected readonly int Iterations;

        protected readonly T[] Values;

        public Runner(string library, string suite, int iterations, IEnumerable<T> values)
        {
            Library = library;
            Iterations = iterations;
            Suite = suite;
            Values = values.ToArray();

            if (Iterations % Values.Length != 0)
            {
                throw new ArgumentException("Iteration count must be a multiple of the number of values.");
            }
        }

        public abstract IEnumerable<(string, TimeSpan)> Run();
    }
}

namespace Chr.Avro.Benchmarks.Apache
{
    using global::Avro;
    using global::Avro.Generic;
    using global::Avro.Specific;
    using global::Avro.IO;

    public abstract class GenericRunner<T> : Benchmarks.Runner<T>
    {
        protected readonly Schema Schema;

        public GenericRunner(string suite, int iterations, string schema, IEnumerable<T> values)
            : base("Confluent.Apache.Avro", suite, iterations, values)
        {
            Schema = Schema.Parse(schema);
        }

        public override IEnumerable<(string, TimeSpan)> Run()
        {
            var stream = new MemoryStream();

            var reader = new GenericDatumReader<T>(Schema, Schema);
            var writer = new GenericDatumWriter<T>(Schema);

            using (stream)
            {
                var encoder = new BinaryEncoder(stream);

                foreach (var value in Values)
                {
                    writer.Write(value, encoder);
                }
            }

            var count = Values.Length;
            var size = stream.ToArray().Length * Iterations / count;

            stream = new MemoryStream(size);

            using (stream)
            {
                var decoder = new BinaryDecoder(stream);
                var encoder = new BinaryEncoder(stream);

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < Iterations; i++)
                {
                    writer.Write(Values[i % count], encoder);
                }

                stopwatch.Stop();
                yield return ("serialization", stopwatch.Elapsed);

                stopwatch.Reset();
                stream.Position = 0;
                stopwatch.Start();

                for (int i = 0; i < Iterations; i++)
                {
                    reader.Read(default, decoder);
                }

                stopwatch.Stop();
                yield return ("deserialization", stopwatch.Elapsed);
            }
        }
    }

    public abstract class SpecificRunner<T> : Benchmarks.Runner<T> where T : new()
    {
        protected readonly Schema Schema;

        public SpecificRunner(string suite, int iterations, string schema, IEnumerable<T> values)
            : base("Confluent.Apache.Avro", suite, iterations, values)
        {
            Schema = Schema.Parse(schema);
        }

        public override IEnumerable<(string, TimeSpan)> Run()
        {
            var stream = new MemoryStream();

            var reader = new SpecificDatumReader<T>(Schema, Schema);
            var writer = new SpecificDatumWriter<T>(Schema);

            using (stream)
            {
                var encoder = new BinaryEncoder(stream);

                foreach (var value in Values)
                {
                    writer.Write(value, encoder);
                }
            }

            var count = Values.Length;
            var size = stream.ToArray().Length * Iterations / count;

            stream = new MemoryStream(size);

            using (stream)
            {
                var decoder = new BinaryDecoder(stream);
                var encoder = new BinaryEncoder(stream);

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < Iterations; i++)
                {
                    writer.Write(Values[i % count], encoder);
                }

                stopwatch.Stop();
                yield return ("serialization", stopwatch.Elapsed);

                stopwatch.Reset();
                stream.Position = 0;
                stopwatch.Start();

                for (int i = 0; i < Iterations; i++)
                {
                    reader.Read(new T(), decoder);
                }

                stopwatch.Stop();
                yield return ("deserialization", stopwatch.Elapsed);
            }
        }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    using global::Chr.Avro.Abstract;
    using global::Chr.Avro.Representation;
    using global::Chr.Avro.Serialization;

    public abstract class Runner<T> : Benchmarks.Runner<T>
    {
        protected readonly Schema Schema;

        public Runner(string suite, int iterations, string schema, IEnumerable<T> values)
            : base("Chr.Avro", suite, iterations, values)
        {
            Schema = new JsonSchemaReader().Read(schema);
        }

        public override IEnumerable<(string, TimeSpan)> Run()
        {
            var stream = new MemoryStream();

            var deserialize = new BinaryDeserializerBuilder().BuildDelegate<T>(Schema);
            var serialize = new BinarySerializerBuilder().BuildDelegate<T>(Schema);

            using (stream)
            {
                foreach (var value in Values)
                {
                    serialize(value, stream);
                }
            }

            var count = Values.Length;
            var size = stream.ToArray().Length * Iterations / count;

            stream = new MemoryStream(size);

            using (stream)
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                for (int i = 0; i < Iterations; i++)
                {
                    serialize(Values[i % count], stream);
                }

                stopwatch.Stop();
                yield return ("serialization", stopwatch.Elapsed);

                stopwatch.Reset();
                stream.Position = 0;
                stopwatch.Start();

                for (int i = 0; i < Iterations; i++)
                {
                    deserialize(stream);
                }

                stopwatch.Stop();
                yield return ("deserialization", stopwatch.Elapsed);
            }
        }
    }
}
