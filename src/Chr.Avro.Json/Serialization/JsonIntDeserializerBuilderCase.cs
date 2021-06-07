namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="IntSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonIntDeserializerBuilderCase : IntDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="IntSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="IntSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="int" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is IntSchema intSchema)
            {
                var getInt32 = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetInt32), Type.EmptyTypes);

                try
                {
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, getInt32), type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {intSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonIntDeserializerBuilderCase)} can only be applied to {nameof(IntSchema)}s."));
            }
        }
    }
}
