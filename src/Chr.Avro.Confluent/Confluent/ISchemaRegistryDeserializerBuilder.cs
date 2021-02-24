namespace Chr.Avro.Confluent
{
    using System.Threading.Tasks;
    using global::Confluent.Kafka;

    /// <summary>
    /// Defines methods to build <see cref="T:IDeserializer{T}" />s based on specific schemas from
    /// a Schema Registry instance.
    /// </summary>
    public interface ISchemaRegistryDeserializerBuilder
    {
        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be deserialized.
        /// </typeparam>
        /// <param name="id">
        /// The ID of the schema that should be used to deserialize data.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the deserializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="IDeserializer{T}" /> based on the schema with ID <paramref name="id" />.
        /// </returns>
        Task<IDeserializer<T>> Build<T>(
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None);

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be deserialized.
        /// </typeparam>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize data. The latest version
        /// of the subject will be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the deserializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="IDeserializer{T}" /> based on the latest schema of subject
        /// <paramref name="subject" />.
        /// </returns>
        Task<IDeserializer<T>> Build<T>(
            string subject,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None);

        /// <summary>
        /// Builds a deserializer for a specific schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be deserialized.
        /// </typeparam>
        /// <param name="subject">
        /// The subject of the schema that should be used to deserialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the deserializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="IDeserializer{T}" /> based on the schema of subject
        /// <paramref name="subject" /> with version <paramref name="version" />.
        /// </returns>
        Task<IDeserializer<T>> Build<T>(
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None);
    }
}
