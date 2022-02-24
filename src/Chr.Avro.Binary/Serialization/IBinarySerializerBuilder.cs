namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build binary Avro serializers for .NET <see cref="Type" />s.
    /// </summary>
    public interface IBinarySerializerBuilder : ISerializerBuilder<BinarySerializerBuilderContext>
    {
        /// <summary>
        /// Builds a delegate that writes a binary-encoded Avro value.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <typeparamref name="T" />.
        /// </param>
        /// <param name="context">
        /// An optional serializer builder context. A context can be provided to predefine
        /// <see cref="Expression" />s for certain <see cref="Type" />-<see cref="Schema" /> pairs
        /// or to grant the caller access to inner results; if no context is provided, an empty
        /// context will be created.
        /// </param>
        /// <returns>
        /// A <see cref="BinarySerializer{T}" /> based on <paramref name="schema" />.
        /// </returns>
        BinarySerializer<T> BuildDelegate<T>(Schema schema, BinarySerializerBuilderContext? context = default);

        /// <summary>
        /// Builds an <see cref="Expression" /> that represents a <see cref="BinarySerializer{T}" />.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <typeparamref name="T" />.
        /// </param>
        /// <param name="context">
        /// An optional serializer builder context. A context can be provided to predefine
        /// <see cref="Expression" />s for certain <see cref="Type" />-<see cref="Schema" /> pairs
        /// or to grant the caller access to inner results; if no context is provided, an empty
        /// context will be created.
        /// </param>
        /// <returns>
        /// An expression representing a <see cref="BinarySerializer{T}" /> based on
        /// <paramref name="schema" />.
        /// </returns>
        Expression<BinarySerializer<T>> BuildDelegateExpression<T>(Schema schema, BinarySerializerBuilderContext? context = default);
    }
}
