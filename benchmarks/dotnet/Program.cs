using CsvHelper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Chr.Avro.Benchmarks
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var csv = new CsvWriter(Console.Out))
            {
                csv.WriteRecords(Run<Apache.SingleBooleanDeserializationRunner>());
                csv.WriteRecords(Run<Chr.SingleBooleanDeserializationRunner>());
                csv.WriteRecords(Run<Apache.SingleBooleanSerializationRunner>());
                csv.WriteRecords(Run<Chr.SingleBooleanSerializationRunner>());
            }
        }

        public static IEnumerable<Result> Run<T>(int iterations = 5) where T : IRunner, new()
        {
            var runner = new T();

            for (int i = 1; i <= iterations; i++)
            {
                yield return new Result()
                {
                    Subject = runner.Subject,
                    Suite = runner.Suite,
                    Iteration = i,
                    Duration = runner.Run().TotalMilliseconds
                };
            }
        }
    }

    public sealed class Result
    {
        public string Subject { get; set; }

        public string Suite { get; set; }

        public int Iteration { get; set; }

        public double Duration { get; set; }
    }

    public interface IRunner
    {
        string Subject { get; }

        string Suite { get; }

        TimeSpan Run();
    }
}
