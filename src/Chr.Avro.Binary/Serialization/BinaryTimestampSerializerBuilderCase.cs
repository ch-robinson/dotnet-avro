namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="TimestampLogicalType" />
    /// and attempts to map it to <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
    /// </summary>
    public class BinaryTimestampSerializerBuilderCase : TimestampSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="TimestampLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimestampLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />,
        /// <see cref="MillisecondTimestampLogicalType" />, or <see cref="NanosecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="DateTimeOffset" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema.LogicalType is TimestampLogicalType)
            {
                if (schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(DateTimeOffset));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                var utcTicks = typeof(DateTimeOffset)
                    .GetProperty(nameof(DateTimeOffset.UtcTicks))!;

                var writeInteger = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) })!;

                return BinarySerializerBuilderCaseResult.FromExpression(
                    Expression.Call(
                        context.Writer,
                        writeInteger,
                        BuildTicksToTimestamp(
                            Expression.Subtract(
                                Expression.Property(expression, utcTicks),
                                Expression.Constant(Epoch.Ticks)),
                            schema)));
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryTimestampSerializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
