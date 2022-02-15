namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build binary Avro deserializers for .NET <see cref="Type" />s.
    /// </summary>
    public interface IBinaryDeserializerBuilder : IDeserializerBuilder<BinaryDeserializerBuilderContext>
    {
        /// <summary>
        /// Builds a delegate that reads a binary-encoded Avro value of a specific
        /// <see cref="Type" />.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> to be deserialized.
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
        /// A <see cref="BinaryDeserializer{T}" /> based on <paramref name="schema" />.
        /// </returns>
        BinaryDeserializer<T> BuildDelegate<T>(Schema schema, BinaryDeserializerBuilderContext? context = default);

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents a <see cref="BinaryDeserializer{T}" />
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
        /// An expression representing a <see cref="BinaryDeserializer{T}" /> based on
        /// <paramref name="schema" />.
        /// </returns>
        Expression<BinaryDeserializer<T>> BuildDelegateExpression<T>(Schema schema, BinaryDeserializerBuilderContext? context = default);
    }
}
