namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="FixedSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonFixedDeserializerBuilderCase : FixedDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="FixedSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="FixedSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="T:System.Byte[]" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is FixedSchema fixedSchema)
            {
                var tokenType = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenType));

                var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                var getString = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                var getBytes = typeof(Encoding)
                    .GetMethod(nameof(Encoding.GetBytes), new[] { typeof(string) });

                var getUnexpectedSizeException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedSizeException));

                var bytes = Expression.Parameter(typeof(byte[]));

                try
                {
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Block(
                                new[] { bytes },
                                Expression.IfThen(
                                    Expression.NotEqual(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.String)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.String })))),
                                Expression.Assign(
                                    bytes,
                                    Expression.Call(
                                        Expression.Constant(JsonEncoding.Bytes),
                                        getBytes,
                                        Expression.Call(context.Reader, getString))),
                                Expression.IfThen(
                                    Expression.NotEqual(
                                        Expression.ArrayLength(bytes),
                                        Expression.Constant(fixedSchema.Size)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedSizeException,
                                            context.Reader,
                                            Expression.Constant(fixedSchema.Size),
                                            Expression.ArrayLength(bytes)))),
                                bytes),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {fixedSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonFixedDeserializerBuilderCase)} can only be applied to {nameof(FixedSchema)}s."));
            }
        }
    }
}
