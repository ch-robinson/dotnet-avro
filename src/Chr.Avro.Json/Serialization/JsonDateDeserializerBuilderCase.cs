#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="DateLogicalType" />
    /// and attempts to map it to <see cref="DateOnly" />.
    /// </summary>
    public class JsonDateDeserializerBuilderCase : DateDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="DateLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DateLogicalType" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="IntSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="DateLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="DateOnly" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DateLogicalType)
            {
                if (schema is not IntSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(DateLogicalType)} deserializers can only be built for {nameof(IntSchema)}s.");
                }

                var getInt32 = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetInt32), Type.EmptyTypes);

                var addDays = typeof(DateOnly)
                    .GetMethod(nameof(DateOnly.AddDays));

                try
                {
                    // return Epoch.AddDays(value);
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(
                                Expression.Constant(Epoch),
                                addDays,
                                Expression.Call(context.Reader, getInt32)),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDateDeserializerBuilderCase)} can only be applied schemas with a {nameof(DateLogicalType)}."));
            }
        }
    }
}
#endif
