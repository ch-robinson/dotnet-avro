#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="DateLogicalType" />
    /// and attempts to map it to <see cref="DateOnly" />.
    /// </summary>
    public class BinaryDateDeserializerBuilderCase : DateDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="DateLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DateLogicalType" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="IntSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="DateLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="DateOnly" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DateLogicalType)
            {
                if (schema is not IntSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(DateLogicalType)} deserializers can only be built for {nameof(IntSchema)}s.");
                }

                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                var addDays = typeof(DateOnly)
                    .GetMethod(nameof(DateOnly.AddDays), new[] { typeof(int) });

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(
                                Expression.Constant(Epoch),
                                addDays,
                                Expression.ConvertChecked(
                                    Expression.Call(context.Reader, readInteger),
                                    typeof(int))),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDateDeserializerBuilderCase)} can only be applied to schemas with a {nameof(DateLogicalType)}."));
            }
        }
    }
}
#endif
