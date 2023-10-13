namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="StringSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonStringSerializerBuilderCase : StringSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="StringSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="StringSchema" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="string" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is StringSchema stringSchema)
            {
                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeString, BuildConversion(value, typeof(string))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {stringSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonStringSerializerBuilderCase)} can only be applied to {nameof(StringSchema)}s."));
            }
        }

        protected virtual Expression BuildConversion(Expression value, Type target)
        {
            var fromString = target
                .GetMethod("FromString", new Type[] { typeof(string) });

            if (fromString != null)
            {
                value = Expression.Call(null, fromString, value);
            }

            return base.BuildConversion(value, target);
        }

    }
}
