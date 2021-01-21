using Chr.Avro.Serialization;
using Confluent.Kafka;
using System;
using System.IO;

namespace Chr.Avro.Confluent
{
    internal class DelegateSerializer<T> : ISerializer<T>
    {
        protected int SchemaId { get; }

        protected TombstoneBehavior TombstoneBehavior { get; }

        private readonly BinarySerializer<T> _delegate;

        public DelegateSerializer(BinarySerializer<T> @delegate, int schemaId, TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            SchemaId = schemaId;
            TombstoneBehavior = tombstoneBehavior;

            _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            if (data == null && TombstoneBehavior != TombstoneBehavior.None)
            {
                if (context.Component == MessageComponentType.Value || TombstoneBehavior != TombstoneBehavior.Strict)
                {
                    return null;
                }
            }

            var header = new byte[5];
            Array.Copy(BitConverter.GetBytes(SchemaId), 0, header, 1, 4);

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(header, 1, 4);
            }

            var stream = new MemoryStream();

            using (stream)
            {
                stream.Write(header, 0, header.Length);
                _delegate(data, new Serialization.BinaryWriter(stream));
            }

            return stream.ToArray();
        }
    }
}
