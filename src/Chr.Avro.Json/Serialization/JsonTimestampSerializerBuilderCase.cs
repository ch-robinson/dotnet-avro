namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="TimestampLogicalType" />
    /// and attempts to map it to <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
    /// </summary>
    public class JsonTimestampSerializerBuilderCase : TimestampSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="TimestampLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimestampLogicalType" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
        /// or a <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="DateTimeOffset" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context, bool registerExpression)
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

                var factor = schema.LogicalType switch
                {
                    MicrosecondTimestampLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                    MillisecondTimestampLogicalType => TimeSpan.TicksPerMillisecond,
                    _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimestampLogicalType)}."),
                };

                var utcTicks = typeof(DateTimeOffset)
                    .GetProperty(nameof(DateTimeOffset.UtcTicks));

                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(long) });

                // return writer.WriteNumber((value.UtcTicks - epoch) / factor);
                return JsonSerializerBuilderCaseResult.FromExpression(
                    Expression.Call(
                        context.Writer,
                        writeNumber,
                        Expression.Divide(
                            Expression.Subtract(
                                Expression.Property(expression, utcTicks),
                                Expression.Constant(Epoch.Ticks)),
                            Expression.Constant(factor))));
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonTimestampSerializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
