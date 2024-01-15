#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="DateLogicalType" />
    /// and attempts to map it to <see cref="DateOnly" />.
    /// </summary>
    public class JsonDateSerializerBuilderCase : DateSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="DateLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DateLogicalType" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
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
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
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

                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(int) });

                // return writer.WriteNumber(value.DayNumber - epoch.DayNumber);
                return JsonSerializerBuilderCaseResult.FromExpression(
                    Expression.Call(
                        context.Writer,
                        writeNumber,
                            Expression.Subtract(
                                Expression.Property(expression, dayNumber),
                                Expression.Constant(Epoch.DayNumber))));
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDateSerializerBuilderCase)} can only be applied schemas with a {nameof(DateLogicalType)}."));
            }
        }
    }
}
#endif
