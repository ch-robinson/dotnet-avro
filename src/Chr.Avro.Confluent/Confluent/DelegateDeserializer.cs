namespace Chr.Avro.Confluent
{
    using System;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;

    /// <summary>
    /// Implements a <see cref="IDeserializer{T}" /> around a <see cref="BinaryDeserializer{T}" />.
    /// </summary>
    /// <typeparam name="T">
    /// The type to be deserialized.
    /// </typeparam>
    internal class DelegateDeserializer<T> : IDeserializer<T>
    {
        private readonly BinaryDeserializer<T> @delegate;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelegateDeserializer{T}" /> class.
        /// </summary>
        /// <param name="delegate">
        /// A deserialization function for <typeparamref name="T" />.
        /// </param>
        /// <param name="schemaId">
        /// The ID of the schema that <paramref name="delegate" /> was built against.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the deserializer should handle tombstone records.
        /// </param>
        public DelegateDeserializer(BinaryDeserializer<T> @delegate, int schemaId, TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            SchemaId = schemaId;
            TombstoneBehavior = tombstoneBehavior;

            this.@delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        /// <summary>
        /// Gets the ID of the schema that the deserializer was built against.
        /// </summary>
        public int SchemaId { get; }

        /// <summary>
        /// Gets an value describing how the deserializer will handle tombstone records.
        /// </summary>
        public TombstoneBehavior TombstoneBehavior { get; }

        /// <inheritdoc />
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
                throw new InvalidEncodingException(0, "The encoded data does not include a Confluent wire format header.");
            }

            var header = data.Slice(0, 5).ToArray();

            if (header[0] != 0x00)
            {
                throw new InvalidEncodingException(0, "The encoded data does not conform to the Confluent wire format.");
            }

            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(header, 1, 4);
            }

            var received = BitConverter.ToInt32(header, 1);

            if (received != SchemaId)
            {
                throw new InvalidEncodingException(1, $"The received schema ({received}) does not match the specified schema ({SchemaId}).");
            }

            var reader = new BinaryReader(data.Slice(5));

            return @delegate(ref reader);
        }
    }
}
