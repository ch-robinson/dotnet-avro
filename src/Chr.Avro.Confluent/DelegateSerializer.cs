using Confluent.Kafka;
using System;
using System.IO;

namespace Chr.Avro.Confluent
{
    internal class DelegateSerializer<T> : ISerializer<T>
    {
        private readonly Action<T, Stream> _delegate;

        public DelegateSerializer(Action<T, Stream> @delegate)
        {
            _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            var stream = new MemoryStream();

            using (stream)
            {
                _delegate(data, stream);
            }

            return stream.ToArray();
        }
    }
}
