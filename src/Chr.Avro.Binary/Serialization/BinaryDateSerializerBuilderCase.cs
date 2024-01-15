#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="DateLogicalType" />
    /// and attempts to map it to <see cref="DateOnly" />.
    /// </summary>
    public class BinaryDateSerializerBuilderCase : DateSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="DateLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DateLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="IntSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="DateLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="DateOnly" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema.LogicalType is DateLogicalType)
            {
                if (schema is not IntSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(DateLogicalType)} serializers can only be built for {nameof(IntSchema)}s.");
                }

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(DateOnly));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                var dayNumber = typeof(DateOnly)
                    .GetProperty(nameof(DateOnly.DayNumber));

                var writeInteger = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(int) });

                // return writer.WriteInteger(value.DayNumber - epoch.DayNumber);
                return BinarySerializerBuilderCaseResult.FromExpression(
                    Expression.Call(
                        context.Writer,
                        writeInteger,
                        Expression.Subtract(
                            Expression.Property(expression, dayNumber),
                            Expression.Constant(Epoch.DayNumber))));
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDateSerializerBuilderCase)} can only be applied schemas with a {nameof(DateLogicalType)}."));
            }
        }
    }
}
#endif
