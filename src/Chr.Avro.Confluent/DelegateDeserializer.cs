using Confluent.Kafka;
using System;
using System.IO;

namespace Chr.Avro.Confluent
{
    internal class DelegateDeserializer<T> : IDeserializer<T>
    {
        private readonly Func<Stream, T> _delegate;

        public DelegateDeserializer(Func<Stream, T> @delegate)
        {
            _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            using (var stream = new MemoryStream(data.ToArray(), false))
            {
                return _delegate(stream);
            }
        }
    }
}
