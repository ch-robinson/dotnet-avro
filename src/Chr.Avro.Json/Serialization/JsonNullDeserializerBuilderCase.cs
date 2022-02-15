namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="NullSchema" />.
    /// </summary>
    public class JsonNullDeserializerBuilderCase : NullDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="NullSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="NullSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is NullSchema)
            {
                var tokenType = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenType));

                var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                return JsonDeserializerBuilderCaseResult.FromExpression(
                    Expression.Block(
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.Property(context.Reader, tokenType),
                                Expression.Constant(JsonTokenType.Null)),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    getUnexpectedTokenException,
                                    context.Reader,
                                    Expression.Constant(new[] { JsonTokenType.Null })))),
                        Expression.Default(type)));
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonNullDeserializerBuilderCase)} can only be applied to {nameof(NullSchema)}s."));
            }
        }
    }
}
