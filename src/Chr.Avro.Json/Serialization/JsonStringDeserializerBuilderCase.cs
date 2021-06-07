namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="StringSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonStringDeserializerBuilderCase : StringDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="StringSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="StringSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="string" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is StringSchema stringSchema)
            {
                var tokenType = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenType));

                var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                var getString = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                try
                {
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.And(
                                        Expression.NotEqual(
                                            Expression.Property(context.Reader, tokenType),
                                            Expression.Constant(JsonTokenType.PropertyName)),
                                        Expression.NotEqual(
                                            Expression.Property(context.Reader, tokenType),
                                            Expression.Constant(JsonTokenType.String))),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.PropertyName, JsonTokenType.String })))),
                                Expression.Call(context.Reader, getString)),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {stringSchema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonStringDeserializerBuilderCase)} can only be applied to {nameof(StringSchema)}s."));
            }
        }
    }
}
