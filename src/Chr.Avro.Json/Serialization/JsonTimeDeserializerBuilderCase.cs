#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="TimeLogicalType" />
    /// and attempts to map it to <see cref="TimeOnly" />.
    /// </summary>
    public class JsonTimeDeserializerBuilderCase : TimeDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="TimeLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimeLogicalType" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> with a
        /// <see cref="MicrosecondTimeLogicalType" /> or when <paramref name="schema" /> is not an
        /// <see cref="IntSchema" /> with a <see cref="MillisecondTimeLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="TimeOnly" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema.LogicalType is TimeLogicalType)
            {
                if (schema.LogicalType is MicrosecondTimeLogicalType && schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(MicrosecondTimeLogicalType)} deserializers can only be built for {nameof(LongSchema)}s.");
                }

                if (schema.LogicalType is MillisecondTimeLogicalType && schema is not IntSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(MillisecondTimeLogicalType)} deserializers can only be built for {nameof(IntSchema)}s.");
                }

                var factor = schema.LogicalType switch
                {
                    MicrosecondTimeLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                    MillisecondTimeLogicalType => TimeSpan.TicksPerMillisecond,
                    _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimeLogicalType)}."),
                };

                var getInt64 = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetInt64), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, getInt64);

                var timeSpanConstructor = typeof(TimeSpan)
                    .GetConstructor(new[] { typeof(long) });

                var add = typeof(TimeOnly)
                    .GetMethod(nameof(TimeOnly.Add), new [] { typeof(TimeSpan) });

                try
                {
                    // return Midnight.Add(new TimeSpan(value * factor));
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(
                                Expression.Constant(Midnight),
                                add,
                                Expression.New(
                                    timeSpanConstructor,
                                    Expression.Multiply(expression, Expression.Constant(factor)))),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonTimeDeserializerBuilderCase)} can only be applied schemas with a {nameof(TimeLogicalType)}."));
            }
        }
    }
}
#endif
