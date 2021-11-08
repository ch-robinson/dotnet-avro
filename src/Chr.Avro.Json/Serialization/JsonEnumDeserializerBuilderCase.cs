namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="EnumSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonEnumDeserializerBuilderCase : EnumDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is an <see cref="EnumSchema" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> is an enum type without a matching member for each
        /// symbol in <paramref name="schema" /> or when <see cref="string" /> cannot be converted
        /// to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                var getString = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, getString);

                var underlying = Nullable.GetUnderlyingType(type) ?? type;

                // enum fields will always be public static, so no need to expose binding flags:
                var fields = underlying.GetFields(BindingFlags.Public | BindingFlags.Static);

                // find a match for each enum in the schema:
                var cases = underlying.IsEnum
                    ? enumSchema.Symbols
                        .Select(symbol =>
                        {
                            var match = fields.SingleOrDefault(field => IsMatch(symbol, field.Name));

                            if (match == null)
                            {
                                throw new UnsupportedTypeException(type, $"{type} has no value that matches {symbol}.");
                            }

                            return Expression.SwitchCase(
                                BuildConversion(Expression.Constant(Enum.Parse(underlying, match.Name)), type),
                                Expression.Constant(symbol));
                        })
                    : enumSchema.Symbols
                        .Select(symbol =>
                        {
                            return Expression.SwitchCase(
                                BuildConversion(Expression.Constant(symbol), type),
                                Expression.Constant(symbol));
                        });

                var position = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenStartIndex))
                    .GetGetMethod();

                var exceptionConstructor = typeof(InvalidEncodingException)
                    .GetConstructor(new[] { typeof(long), typeof(string), typeof(Exception) });

                // generate a switch on the symbol:
                return JsonDeserializerBuilderCaseResult.FromExpression(
                    Expression.Switch(
                        expression,
                        Expression.Throw(
                            Expression.New(
                                exceptionConstructor,
                                Expression.Property(context.Reader, position),
                                Expression.Constant($"Invalid enum symbol."),
                                Expression.Constant(null, typeof(Exception))),
                            type),
                        cases.ToArray()));
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonEnumDeserializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
