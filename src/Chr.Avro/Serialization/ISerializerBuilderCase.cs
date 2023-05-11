namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build Avro serializers for specific
    /// <see cref="Type" />-<see cref="Schema" /> pairs.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as the build operation progresses.
    /// </typeparam>
    /// <typeparam name="TResult">
    /// The type of object used to represent the case result.
    /// </typeparam>
    public interface ISerializerBuilderCase<TContext, TResult>
    {
        /// <summary>
        /// Builds a serializer for a <see cref="Type" />-<see cref="Schema" /> pair.
        /// </summary>
        /// <param name="value">
        /// An <see cref="Expression" /> representing the value to be serialized.
        /// </param>
        /// <param name="type">
        /// The <see cref="Type" /> to be serialized.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <paramref name="type" />.
        /// </param>
        /// <param name="context">
        /// A <typeparamref name="TContext" /> representing the state of the build operation.
        /// </param>
        /// <param name="registerExpression">
        /// Indicates whether to register the computed expression as the serializer for the type for future calls.
        /// </param>
        /// <returns>
        /// A successful <typeparamref name="TResult" /> if the case can be applied;
        /// an unsuccessful <typeparamref name="TResult" /> otherwise.
        /// </returns>
        TResult BuildExpression(Expression value, Type type, Schema schema, TContext context, bool registerExpression);
    }
}
