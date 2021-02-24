namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="EnumSchema" />
    /// and attempts to map it to enum types.
    /// </summary>
    public class JsonEnumSerializerBuilderCase : EnumSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is an <see cref="EnumResolution" /> and <paramref name="schema" /> is an <see cref="EnumSchema" />;
        /// an unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="schema" /> does not have a matching symbol for each member
        /// of the resolved <see cref="Type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var writeString = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

                    var symbols = enumSchema.Symbols.ToList();

                    // find a match for each enum in the type:
                    var cases = enumResolution.Symbols.Select(symbol =>
                    {
                        var match = symbols.Find(s => symbol.Name.IsMatch(s));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has a symbol ({symbol.Name}) that cannot be serialized.");
                        }

                        if (symbols.FindLast(s => symbol.Name.IsMatch(s)) != match)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has an ambiguous symbol ({symbol.Name}).");
                        }

                        return Expression.SwitchCase(
                            Expression.Call(context.Writer, writeString, Expression.Constant(match)),
                            Expression.Constant(symbol.Value));
                    });

                    var exceptionConstructor = typeof(ArgumentOutOfRangeException)
                        .GetConstructor(new[] { typeof(string) });

                    var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum value out of range."));

                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Switch(value, Expression.Throw(exception), cases.ToArray()));
                }
                else
                {
                    return JsonSerializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonEnumSerializerBuilderCase)} can only be applied to {nameof(EnumResolution)}s."));
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonEnumSerializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
