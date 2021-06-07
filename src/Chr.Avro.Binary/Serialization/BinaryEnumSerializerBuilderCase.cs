namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="EnumSchema" />
    /// and attempts to map it to enum types.
    /// </summary>
    public class BinaryEnumSerializerBuilderCase : EnumSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for an <see cref="EnumSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enum and <paramref name="schema" /> is an <see cref="EnumSchema" />; an
        /// unsuccessful <see cref="BinarySerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="schema" /> does not have a matching symbol for each member
        /// of <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                if (type.IsEnum)
                {
                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                    // enum fields will always be public static, so no need to expose binding flags:
                    var fields = type.GetFields(BindingFlags.Public | BindingFlags.Static);
                    var symbols = enumSchema.Symbols.ToList();

                    // find a match for each enum in the type:
                    var cases = fields.Select(field =>
                    {
                        var index = symbols.FindIndex(symbol => IsMatch(symbol, field.Name));

                        if (index < 0)
                        {
                            throw new UnsupportedTypeException(type, $"{type} has a field {field.Name} that cannot be serialized.");
                        }

                        if (symbols.FindLastIndex(symbol => IsMatch(symbol, field.Name)) != index)
                        {
                            throw new UnsupportedTypeException(type, $"{type} has an ambiguous field {field.Name}.");
                        }

                        return Expression.SwitchCase(
                            Expression.Call(context.Writer, writeInteger, Expression.Constant((long)index)),
                            Expression.Constant(Enum.Parse(type, field.Name)));
                    });

                    var exceptionConstructor = typeof(ArgumentOutOfRangeException)
                        .GetConstructor(new[] { typeof(string) });

                    var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum value out of range."));

                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Switch(value, Expression.Throw(exception), cases.ToArray()));
                }
                else
                {
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryEnumSerializerBuilderCase)} can only be applied to enum types."));
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryEnumSerializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
