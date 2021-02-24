namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="EnumSchema" />
    /// and attempts to map it to enum types.
    /// </summary>
    public class JsonEnumDeserializerBuilderCase : EnumDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is an <see ref="EnumResolution" /> and <paramref name="schema" /> is an <see cref="EnumSchema" />;
        /// an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="resolution" /> does not contain a matching symbol for each
        /// symbol in <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var getString = typeof(Utf8JsonReader)
                        .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                    Expression expression = Expression.Call(context.Reader, getString);

                    // find a match for each enum in the schema:
                    var cases = enumSchema.Symbols.Select(name =>
                    {
                        var match = enumResolution.Symbols.SingleOrDefault(s => s.Name.IsMatch(name));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has no value that matches {name}.");
                        }

                        return Expression.SwitchCase(
                            BuildConversion(Expression.Constant(match.Value), resolution.Type),
                            Expression.Constant(name));
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
                                resolution.Type),
                            cases.ToArray()));
                }
                else
                {
                    return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonEnumDeserializerBuilderCase)} can only be applied to {nameof(EnumResolution)}s."));
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonEnumDeserializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
