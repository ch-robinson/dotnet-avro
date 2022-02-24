namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="BooleanSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonBooleanDeserializerBuilderCase : BooleanDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="BooleanSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="BooleanSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="bool" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is BooleanSchema booleanSchema)
            {
                var getBoolean = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetBoolean), Type.EmptyTypes);

                try
                {
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, getBoolean), type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {booleanSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonBooleanDeserializerBuilderCase)} can only be applied to {nameof(BooleanSchema)}s."));
            }
        }
    }
}
