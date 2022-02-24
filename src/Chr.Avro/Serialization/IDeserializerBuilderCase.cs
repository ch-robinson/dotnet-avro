namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build Avro deserializers for specific
    /// <see cref="Type" />-<see cref="Schema" /> pairs.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as the build operation progresses.
    /// </typeparam>
    /// <typeparam name="TResult">
    /// The type of object used to represent the case result.
    /// </typeparam>
    public interface IDeserializerBuilderCase<TContext, TResult>
    {
        /// <summary>
        /// Builds a deserializer for a <see cref="Type" />-<see cref="Schema" /> pair.
        /// </summary>
        /// <param name="type">
        /// The <see cref="Type" /> to be deserialized.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <paramref name="type" />.
        /// </param>
        /// <param name="context">
        /// A <typeparamref name="TContext" /> representing the state of the build operation.
        /// </param>
        /// <returns>
        /// A successful <typeparamref name="TResult" /> if the case can be applied;
        /// an unsuccessful <typeparamref name="TResult" /> otherwise.
        /// </returns>
        TResult BuildExpression(Type type, Schema schema, TContext context);
    }
}
