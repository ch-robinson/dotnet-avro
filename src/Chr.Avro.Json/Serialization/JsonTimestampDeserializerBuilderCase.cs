namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// has a <see cref="TimestampLogicalType" /> and <paramref name="resolution" /> is a
        /// <see cref="TimestampResolution" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
        /// or a <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="DateTime" /> cannot be converted to the resolved type.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema.LogicalType is TimestampLogicalType)
            {
                if (resolution is TimestampResolution)
                {
                    if (!(schema is LongSchema))
                    {
                        throw new UnsupportedSchemaException(schema);
                    }

                    Expression epoch = Expression.Constant(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc));
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
                                resolution.Type));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(resolution.Type, $"Failed to map {schema} to {resolution.Type}.", exception);
                    }
                }
                else
                {
                    return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonTimestampDeserializerBuilderCase)} can only be applied to {nameof(TimestampResolution)}s."));
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonTimestampDeserializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
