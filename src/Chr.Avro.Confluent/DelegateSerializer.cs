using Confluent.Kafka;
using System;

namespace Chr.Avro.Confluent
{
    internal class DelegateSerializer<T> : ISerializer<T>
    {
        private readonly Func<T, SerializationContext, byte[]> _delegate;

        public DelegateSerializer(Func<T, SerializationContext, byte[]> @delegate)
        {
            _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            return _delegate(data, context);
        }
    }
}
