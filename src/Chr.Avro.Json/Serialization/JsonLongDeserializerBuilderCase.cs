namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="LongSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonLongDeserializerBuilderCase : LongDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="LongSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="LongSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="long" /> cannot be converted to the resolved <see cref="Type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is LongSchema longSchema)
            {
                var getInt64 = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetInt64), Type.EmptyTypes);

                try
                {
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, getInt64), resolution.Type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"Failed to map {longSchema} to {resolution.Type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonLongDeserializerBuilderCase)} can only be applied to {nameof(LongSchema)}s."));
            }
        }
    }
}
