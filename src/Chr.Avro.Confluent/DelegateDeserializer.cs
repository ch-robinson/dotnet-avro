using Confluent.Kafka;
using System;

namespace Chr.Avro.Confluent
{
    internal class DelegateDeserializer<T> : IDeserializer<T>
    {
        private readonly Func<byte[], bool, SerializationContext, T> _delegate;

        public DelegateDeserializer(Func<byte[], bool, SerializationContext, T> @delegate)
        {
            _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            return _delegate(data.ToArray(), isNull, context);
        }
    }
}
