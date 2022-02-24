namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build JSON Avro deserializers for .NET <see cref="Type" />s.
    /// </summary>
    public interface IJsonDeserializerBuilder : IDeserializerBuilder<JsonDeserializerBuilderContext>
    {
        /// <summary>
        /// Builds a delegate that reads a JSON-encoded Avro value.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> of object to be deserialized.
        /// </typeparam>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <typeparamref name="T" />.
        /// </param>
        /// <param name="context">
        /// An optional deserializer builder context. A context can be provided to predefine
        /// <see cref="Expression" />s for certain <see cref="Type" />-<see cref="Schema" /> pairs
        /// or to grant the caller access to inner results; if no context is provided, an empty
        /// context will be created.
        /// </param>
        /// <returns>
        /// A <see cref="JsonDeserializer{T}" /> based on <paramref name="schema" />.
        /// </returns>
        JsonDeserializer<T> BuildDelegate<T>(Schema schema, JsonDeserializerBuilderContext? context = default);

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents a <see cref="JsonDeserializer{T}" />
        /// for a specific <see cref="Type" />.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> of object to be deserialized.
        /// </typeparam>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <typeparamref name="T" />.
        /// </param>
        /// <param name="context">
        /// An optional deserializer builder context. A context can be provided to predefine
        /// <see cref="Expression" />s for certain <see cref="Type" />-<see cref="Schema" /> pairs
        /// or to grant the caller access to inner results; if no context is provided, an empty
        /// context will be created.
        /// </param>
        /// <returns>
        /// An expression representing a <see cref="JsonDeserializer{T}" /> based on
        /// <paramref name="schema" />.
        /// </returns>
        Expression<JsonDeserializer<T>> BuildDelegateExpression<T>(Schema schema, JsonDeserializerBuilderContext? context = default);
    }
}
