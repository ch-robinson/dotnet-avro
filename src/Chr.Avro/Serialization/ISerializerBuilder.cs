namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build Avro serializers for .NET <see cref="Type" />s.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as a build operation progresses.
    /// </typeparam>
    public interface ISerializerBuilder<TContext>
    {
        /// <summary>
        /// Builds an <see cref="Expression" /> that represents writing <paramref name="value" />.
        /// </summary>
        /// <param name="value">
        /// An <see cref="Expression" /> representing the value to be serialized.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to the <see cref="Expression.Type" /> of <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// A serializer builder context.
        /// </param>
        /// <param name="registerExpression">
        /// Indicates whether to register the computed expression as the serializer for the type for future calls.
        /// </param>
        /// <returns>
        /// An expression representing the serialization of <paramref name="value" /> based on
        /// <paramref name="schema" />.
        /// </returns>
        Expression BuildExpression(Expression value, Schema schema, TContext context, bool registerExpression /*= true*/);
    }
}
