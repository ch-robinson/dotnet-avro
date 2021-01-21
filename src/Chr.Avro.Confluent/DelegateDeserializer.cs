using Chr.Avro.Serialization;
using Confluent.Kafka;
using System;
using System.IO;

namespace Chr.Avro.Confluent
{
    internal class DelegateDeserializer<T> : IDeserializer<T>
    {
        protected int SchemaId { get; }

        protected TombstoneBehavior TombstoneBehavior { get; }

        private readonly BinaryDeserializer<T> _delegate;

        public DelegateDeserializer(BinaryDeserializer<T> @delegate, int schemaId, TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            SchemaId = schemaId;
            TombstoneBehavior = tombstoneBehavior;

            _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull && TombstoneBehavior != TombstoneBehavior.None)
            {
                if (context.Component == MessageComponentType.Value || TombstoneBehavior != TombstoneBehavior.Strict)
                {
                    return default;
                }
            }

            if (data.Length < 5)
            {
                throw new InvalidDataException("The encoded data does not include a Confluent wire format header.");
            }

            var header = data.Slice(0, 5).ToArray();

            if (header[0] != 0x00)
            {
                throw new InvalidDataException("The encoded data does not conform to the Confluent wire format.");
            }

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(header, 1, 4);
            }

            var received = BitConverter.ToInt32(header, 1);

            if (received != SchemaId)
            {
                throw new InvalidDataException($"The received schema ({received}) does not match the specified schema ({SchemaId}).");
            }

            var reader = new Serialization.BinaryReader(data.Slice(5));

            return _delegate(ref reader);
        }
    }
}
