using System;
using System.Diagnostics;
using System.IO;

namespace Chr.Avro.Benchmarks.Apache
{
    using global::Avro;
    using global::Avro.IO;
    using global::Avro.Generic;

    public class SingleBooleanDeserializationRunner : IRunner
    {
        public string Subject => "Confluent.Apache.Avro (.NET Core 2.1)";

        public string Suite => $"single boolean deserialization, {_iterations} iterations";

        private readonly byte[] _bytes;

        private readonly int _iterations = 10_000_000;

        private readonly DatumReader<bool> _reader;

        public SingleBooleanDeserializationRunner()
        {
            _bytes = new byte[_iterations];

            for (var i = 0; i < _bytes.Length; i++)
            {
                _bytes[i] = 0x01;
            }

            var schema = Schema.Parse("\"boolean\"");
            _reader = new GenericDatumReader<bool>(schema, schema);
        }

        public TimeSpan Run()
        {
            var stopwatch = new Stopwatch();

            using (var stream = new MemoryStream(_bytes, false))
            {
                var decoder = new BinaryDecoder(stream);

                stopwatch.Start();

                for (int i = 0; i < _iterations; i++)
                {
                    _reader.Read(default(bool), decoder);
                }

                stopwatch.Stop();
            }

            return stopwatch.Elapsed;
        }
    }

    public class SingleBooleanSerializationRunner : IRunner
    {
        public string Subject => "Confluent.Apache.Avro (.NET Core 2.1)";

        public string Suite => $"single boolean serialization, {_iterations} iterations";

        private readonly int _iterations = 10_000_000;

        private readonly DatumWriter<bool> _writer;

        public SingleBooleanSerializationRunner()
        {
            var schema = Schema.Parse("\"boolean\"");
            _writer = new GenericDatumWriter<bool>(schema);
        }

        public TimeSpan Run()
        {
            var stopwatch = new Stopwatch();
            var value = true;

            using (var stream = new MemoryStream(new byte[_iterations]))
            {
                var encoder = new BinaryEncoder(stream);

                stopwatch.Start();

                for (int i = 0; i < _iterations; i++)
                {
                    _writer.Write(value, encoder);
                }

                stopwatch.Stop();
            }

            return stopwatch.Elapsed;
        }
    }
}

namespace Chr.Avro.Benchmarks.Chr
{
    using global::Chr.Avro.Abstract;
    using global::Chr.Avro.Serialization;

    public class SingleBooleanDeserializationRunner : IRunner
    {
        public string Subject => "Chr.Avro (.NET Core 2.1)";

        public string Suite => $"single boolean deserialization, {_iterations} iterations";

        private readonly byte[] _bytes;

        private readonly Func<Stream, bool> _deserializer;

        private readonly int _iterations = 10_000_000;

        public SingleBooleanDeserializationRunner()
        {
            _bytes = new byte[_iterations];

            for (var i = 0; i < _bytes.Length; i++)
            {
                _bytes[i] = 0x01;
            }

            var schema = new BooleanSchema();
            _deserializer = new BinaryDeserializerBuilder().BuildDelegate<bool>(schema);
        }

        public TimeSpan Run()
        {
            var stopwatch = new Stopwatch();

            using (var stream = new MemoryStream(_bytes, false))
            {
                stopwatch.Start();

                for (int i = 0; i < _iterations; i++)
                {
                    _deserializer(stream);
                }

                stopwatch.Stop();
            }

            return stopwatch.Elapsed;
        }
    }

    public class SingleBooleanSerializationRunner : IRunner
    {
        public string Subject => "Chr.Avro (.NET Core 2.1)";

        public string Suite => $"single boolean serialization, {_iterations} iterations";

        private readonly int _iterations = 10_000_000;

        private readonly Action<bool, Stream> _serializer;

        public SingleBooleanSerializationRunner()
        {
            var schema = new BooleanSchema();
            _serializer = new BinarySerializerBuilder().BuildDelegate<bool>(schema);
        }

        public TimeSpan Run()
        {
            var stopwatch = new Stopwatch();
            var value = true;

            using (var stream = new MemoryStream(new byte[_iterations]))
            {
                stopwatch.Start();

                for (int i = 0; i < _iterations; i++)
                {
                    _serializer(value, stream);
                }

                stopwatch.Stop();
            }

            return stopwatch.Elapsed;
        }
    }
}
