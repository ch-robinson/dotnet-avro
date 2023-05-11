namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="LongSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonLongSerializerBuilderCase : LongSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="LongSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="LongSchema" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="long" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context, bool registerExpression)
        {
            if (schema is LongSchema longSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(long) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeNumber, BuildConversion(value, typeof(long))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {longSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonLongSerializerBuilderCase)} can only be applied to {nameof(LongSchema)}s."));
            }
        }
    }
}
