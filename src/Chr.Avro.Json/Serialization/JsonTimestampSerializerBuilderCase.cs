namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// has a <see cref="TimestampLogicalType" /> and <paramref name="resolution" /> is a
        /// <see cref="TimestampResolution" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
        /// or a <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> cannot be converted to <see cref="DateTimeOffset" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema.LogicalType is TimestampLogicalType)
            {
                if (resolution is TimestampResolution)
                {
                    if (!(schema is LongSchema))
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
                        throw new UnsupportedTypeException(resolution.Type, $"Failed to map {schema} to {resolution.Type}.", exception);
                    }

                    var factor = schema.LogicalType switch
                    {
                        MicrosecondTimestampLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                        MillisecondTimestampLogicalType => TimeSpan.TicksPerMillisecond,
                        _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimestampLogicalType)}.")
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
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonTimestampSerializerBuilderCase)} can only be applied to {nameof(TimestampResolution)}s."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonTimestampSerializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
