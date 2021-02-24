namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Defines methods to build Avro deserializers for .NET <see cref="Type" />s.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as a build operation progresses.
    /// </typeparam>
    public interface IDeserializerBuilder<TContext>
    {
        /// <summary>
        /// Builds an <see cref="Expression" /> that represents reading an object of
        /// <paramref name="type" />.
        /// </summary>
        /// <param name="type">
        /// The <see cref="Type" /> of object to be deserialized.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> to map to <paramref name="type" />.
        /// </param>
        /// <param name="context">
        /// A deserializer builder context.
        /// </param>
        /// <returns>
        /// An expression representing the deserialization of <paramref name="type" /> based on
        /// <paramref name="schema" />.
        /// </returns>
        Expression BuildExpression(Type type, Schema schema, TContext context);
    }
}
