namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="TimestampLogicalType" />
    /// and attempts to map it to <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
    /// </summary>
    public class JsonTimestampDeserializerBuilderCase : TimestampDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="TimestampLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimestampLogicalType" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
        /// or a <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="DateTime" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema.LogicalType is TimestampLogicalType)
            {
                if (schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                Expression epoch = Expression.Constant(Epoch);
                Expression factor;

                if (schema.LogicalType is MicrosecondTimestampLogicalType)
                {
                    factor = Expression.Constant(TimeSpan.TicksPerMillisecond / 1000);
                }
                else if (schema.LogicalType is MillisecondTimestampLogicalType)
                {
                    factor = Expression.Constant(TimeSpan.TicksPerMillisecond);
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var getInt64 = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetInt64), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, getInt64);

                var addTicks = typeof(DateTime)
                    .GetMethod(nameof(DateTime.AddTicks));

                try
                {
                    // return Epoch.AddTicks(value * factor);
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(epoch, addTicks, Expression.Multiply(expression, factor)),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonTimestampDeserializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
