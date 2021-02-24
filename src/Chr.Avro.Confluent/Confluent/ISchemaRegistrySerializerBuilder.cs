namespace Chr.Avro.Confluent
{
    using System.Threading.Tasks;
    using global::Confluent.Kafka;

    /// <summary>
    /// Defines methods to build <see cref="T:ISerializer{T}" />s based on specific schemas from
    /// a Schema Registry instance.
    /// </summary>
    public interface ISchemaRegistrySerializerBuilder
    {
        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be serialized.
        /// </typeparam>
        /// <param name="id">
        /// The ID of the schema that should be used to serialize data.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="ISerializer{T}" /> based on the schema with ID <paramref name="id" />.
        /// </returns>
        Task<ISerializer<T>> Build<T>(
            int id,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None);

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be serialized.
        /// </typeparam>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data. The latest version of
        /// the subject will be resolved.
        /// </param>
        /// <param name="registerAutomatically">
        /// When to automatically register a schema that matches <typeparamref name="T" />.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="ISerializer{T}" /> based on the latest schema of subject
        /// <paramref name="subject" />.
        /// </returns>
        Task<ISerializer<T>> Build<T>(
            string subject,
            AutomaticRegistrationBehavior registerAutomatically = AutomaticRegistrationBehavior.Never,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None);

        /// <summary>
        /// Builds a serializer for a specific schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to be serialized.
        /// </typeparam>
        /// <param name="subject">
        /// The subject of the schema that should be used to serialize data.
        /// </param>
        /// <param name="version">
        /// The version of the subject to be resolved.
        /// </param>
        /// <param name="tombstoneBehavior">
        /// The behavior of the serializer on tombstone records.
        /// </param>
        /// <returns>
        /// A <see cref="ISerializer{T}" /> based on the schema of subject
        /// <paramref name="subject" /> with version <paramref name="version" />.
        /// </returns>
        Task<ISerializer<T>> Build<T>(
            string subject,
            int version,
            TombstoneBehavior tombstoneBehavior = TombstoneBehavior.None);
    }
}
