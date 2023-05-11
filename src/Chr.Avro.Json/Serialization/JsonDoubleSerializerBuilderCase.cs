namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="DoubleSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonDoubleSerializerBuilderCase : DoubleSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="DoubleSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="DoubleSchema" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="double" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context, bool registerExpression)
        {
            if (schema is DoubleSchema doubleSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(double) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeNumber, BuildConversion(value, typeof(double))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {doubleSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDoubleSerializerBuilderCase)} can only be applied to {nameof(DoubleSchema)}s."));
            }
        }
    }
}
