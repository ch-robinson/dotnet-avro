#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="TimeLogicalType" />
    /// and attempts to map it to <see cref="TimeOnly" />.
    /// </summary>
    public class JsonTimeSerializerBuilderCase : TimeSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="TimeLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimeLogicalType" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> with a
        /// <see cref="MicrosecondTimeLogicalType" /> or when <paramref name="schema" /> is not an
        /// <see cref="IntSchema" /> with a <see cref="MillisecondTimeLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="TimeOnly" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema.LogicalType is TimeLogicalType)
            {
                if (schema.LogicalType is MicrosecondTimeLogicalType && schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(MicrosecondTimeLogicalType)} serializers can only be built for {nameof(LongSchema)}s.");
                }

                if (schema.LogicalType is MillisecondTimeLogicalType && schema is not IntSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(MillisecondTimeLogicalType)} serializers can only be built for {nameof(IntSchema)}s.");
                }

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(TimeOnly));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                var factor = schema.LogicalType switch
                {
                    MicrosecondTimeLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                    MillisecondTimeLogicalType => TimeSpan.TicksPerMillisecond,
                    _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimeLogicalType)}."),
                };

                var ticks = typeof(TimeOnly)
                    .GetProperty(nameof(TimeOnly.Ticks));

                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(long) });

                // return writer.WriteNumber((value.Ticks - midnight.Ticks) / factor);
                return JsonSerializerBuilderCaseResult.FromExpression(
                    Expression.Call(
                        context.Writer,
                        writeNumber,
                        Expression.Divide(
                            Expression.Subtract(
                                Expression.Property(expression, ticks),
                                Expression.Constant(Midnight.Ticks)),
                            Expression.Constant(factor))));
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonTimeSerializerBuilderCase)} can only be applied schemas with a {nameof(TimeLogicalType)}."));
            }
        }
    }
}
#endif
