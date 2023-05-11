namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="FloatSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonFloatSerializerBuilderCase : FloatSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="FloatSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="FloatSchema" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="float" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context, bool registerExpression)
        {
            if (schema is FloatSchema floatSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(float) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeNumber, BuildConversion(value, typeof(float))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {floatSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonFloatSerializerBuilderCase)} can only be applied to {nameof(FloatSchema)}s."));
            }
        }
    }
}
