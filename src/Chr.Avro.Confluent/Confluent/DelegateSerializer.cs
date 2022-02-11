namespace Chr.Avro.Confluent
{
    using System;
    using System.IO;
    using Chr.Avro.Serialization;
    using global::Confluent.Kafka;

    /// <summary>
    /// Implements a <see cref="ISerializer{T}" /> around a <see cref="BinarySerializer{T}" />.
    /// </summary>
    /// <typeparam name="T">
    /// The type to be serialized.
    /// </typeparam>
    internal class DelegateSerializer<T> : ISerializer<T>
    {
        private readonly Implementation @delegate;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelegateSerializer{T}" /> class.
        /// </summary>
        /// <param name="delegate">
        /// A serialization function for <typeparamref name="T" />.
        /// </param>
        /// <param name="schemaId">
        /// The ID of the schema that <paramref name="delegate" /> was built against.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// How the serializer should handle tombstone records.
        /// </param>
        public DelegateSerializer(Implementation @delegate, int schemaId, TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None)
        {
            SchemaId = schemaId;
            TombstoneBehavior = tombstoneBehavior;

            this.@delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
        }

        /// <summary>
        /// A function that provides the core deserializer implementation.
        /// </summary>
        /// <param name="value">
        /// The object to be serialized.
        /// </param>
        /// <param name="stream">
        /// A <see cref="Stream" /> to write the serialized data to.
        /// </param>
        public delegate void Implementation(T value, Stream stream);

        /// <summary>
        /// Gets the ID of the schema that the serializer was built against.
        /// </summary>
        public int SchemaId { get; }

        /// <summary>
        /// Gets an value describing how the serializer will handle tombstone records.
        /// </summary>
        public TombstoneBehavior TombstoneBehavior { get; }

        /// <inheritdoc />
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
                @delegate(data, stream);
            }

            return stream.ToArray();
        }
    }
}
