namespace Chr.Avro.Codegen.Tests
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    internal class CompatibilityChecker
    {
        /// <summary>
        /// This implementation is an untyped version of <see cref="BinaryDeserializerBuilder.BuildDelegateExpression{T}(Schema, BinaryDeserializerBuilderContext?)"/>
        /// to build a deserializing expression for a type.
        /// </summary>
        /// <param name="type">
        /// Type to check compatibility for.
        /// </param>
        /// <param name="schema">
        /// Schema to check compatibility for the given type against.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <paramref name="type" /> to <paramref name="schema" />.
        /// </exception>
        internal static void AssertCanDeserializeTypeFromSchema(Type type, Schema schema)
        {
            var context = new BinaryDeserializerBuilderContext();

            var root = new BinaryDeserializerBuilder().BuildExpression(type, schema, context);

            // Compile() will throw if not compatible
            Expression.Lambda(
                Expression.Block(
                 context.Assignments.Keys,
                 context.Assignments
                    .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                    .Concat(new[] { root })),
                new[] { context.Reader }).Compile();
        }
    }
}
