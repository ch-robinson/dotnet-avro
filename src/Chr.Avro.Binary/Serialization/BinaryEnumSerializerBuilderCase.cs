namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="EnumSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryEnumSerializerBuilderCase : EnumSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is an <see cref="EnumSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> is an enum type and <paramref name="schema" />
        /// does not have a matching symbol for each member or when <paramref name="type" /> cannot
        /// be converted to <see cref="string" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                var writeInteger = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                var underlying = Nullable.GetUnderlyingType(type) ?? type;

                // enum fields will always be public static, so no need to expose binding flags:
                var fields = type.GetFields(BindingFlags.Public | BindingFlags.Static);
                var symbols = enumSchema.Symbols.ToList();

                var cases = type.IsEnum
                    ? fields
                        .Select(field =>
                        {
                            var index = symbols.FindIndex(symbol => IsMatch(symbol, field));

                            if (index < 0)
                            {
                                throw new UnsupportedTypeException(type, $"{type} has a field {field.Name} that cannot be serialized.");
                            }

                            if (symbols.FindLastIndex(symbol => IsMatch(symbol, field)) != index)
                            {
                                throw new UnsupportedTypeException(type, $"{type} has an ambiguous field {field.Name}.");
                            }

                            return Expression.SwitchCase(
                                Expression.Call(context.Writer, writeInteger, Expression.Constant((long)index)),
                                Expression.Constant(Enum.Parse(type, field.Name)));
                        })
                    : symbols
                        .Select((symbol, index) =>
                        {
                            return Expression.SwitchCase(
                                Expression.Call(context.Writer, writeInteger, Expression.Constant((long)index)),
                                Expression.Constant(symbol));
                        });

                var exceptionConstructor = typeof(ArgumentOutOfRangeException)
                    .GetConstructor(new[] { typeof(string) });

                var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum value out of range."));
                var intermediate = Expression.Variable(type.IsEnum ? type : typeof(string));

                return BinarySerializerBuilderCaseResult.FromExpression(
                    Expression.Block(
                        new[] { intermediate },
                        Expression.Assign(intermediate, BuildConversion(value, intermediate.Type)),
                        Expression.Switch(intermediate, Expression.Throw(exception), cases.ToArray())));
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryEnumSerializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
