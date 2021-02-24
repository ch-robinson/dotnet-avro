namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// <param name="resolution">
        /// The <see cref="TypeResolution" /> to extract <see cref="Type" /> information from.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to the <see cref="Type" /> represented by
        /// <paramref name="resolution" />.
        /// </param>
        /// <param name="context">
        /// A <typeparamref name="TContext" /> representing the state of the build operation.
        /// </param>
        /// <returns>
        /// A successful <typeparamref name="TResult" /> if the case can be applied;
        /// an unsuccessful <typeparamref name="TResult" /> otherwise.
        /// </returns>
        TResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, TContext context);
    }
}
