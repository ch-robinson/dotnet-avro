namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;

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
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enum and <paramref name="schema" /> is an <see cref="EnumSchema" />; an
        /// unsuccessful <see cref="JsonSerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="schema" /> does not have a matching symbol for each member
        /// of <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

                // enum fields will always be public static, so no need to expose binding flags:
                var fields = type.GetFields(BindingFlags.Public | BindingFlags.Static);
                var symbols = enumSchema.Symbols.ToList();

                // find a match for each enum in the type:
                var cases = type.IsEnum
                    ? fields
                        .Select(field =>
                        {
                            var match = symbols.Find(symbol => IsMatch(symbol, field))
                                ?? throw new UnsupportedTypeException(type, $"{type} has a field {field.Name} that cannot be serialized.");

                            if (symbols.FindLast(symbol => IsMatch(symbol, field)) != match)
                            {
                                throw new UnsupportedTypeException(type, $"{type.Name} has an ambiguous field {field.Name}.");
                            }

                            return Expression.SwitchCase(
                                Expression.Call(context.Writer, writeString, Expression.Constant(match)),
                                Expression.Constant(Enum.Parse(type, field.Name)));
                        })
                    : symbols
                        .Select(symbol =>
                        {
                            return Expression.SwitchCase(
                                Expression.Call(context.Writer, writeString, Expression.Constant(symbol)),
                                Expression.Constant(symbol));
                        });

                var exceptionConstructor = typeof(ArgumentOutOfRangeException)
                    .GetConstructor(new[] { typeof(string) });

                var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum value out of range."));
                var intermediate = Expression.Variable(type.IsEnum ? type : typeof(string));

                return JsonSerializerBuilderCaseResult.FromExpression(
                    Expression.Block(
                        new[] { intermediate },
                        Expression.Assign(intermediate, BuildConversion(value, intermediate.Type)),
                        Expression.Switch(intermediate, Expression.Throw(exception), cases.ToArray())));
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonEnumSerializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
