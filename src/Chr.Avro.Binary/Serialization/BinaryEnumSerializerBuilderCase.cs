namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is an <see cref="EnumResolution" /> and <paramref name="schema" /> is an <see cref="EnumSchema" />;
        /// an unsuccessful <see cref="BinarySerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="schema" /> does not have a matching symbol for each member
        /// of the resolved <see cref="Type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                    var symbols = enumSchema.Symbols.ToList();

                    // find a match for each enum in the type:
                    var cases = enumResolution.Symbols.Select(symbol =>
                    {
                        var index = symbols.FindIndex(s => symbol.Name.IsMatch(s));

                        if (index < 0)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has a symbol ({symbol.Name}) that cannot be serialized.");
                        }

                        if (symbols.FindLastIndex(s => symbol.Name.IsMatch(s)) != index)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has an ambiguous symbol ({symbol.Name}).");
                        }

                        return Expression.SwitchCase(
                            Expression.Call(context.Writer, writeInteger, Expression.Constant((long)index)),
                            Expression.Constant(symbol.Value));
                    });

                    var exceptionConstructor = typeof(ArgumentOutOfRangeException)
                        .GetConstructor(new[] { typeof(string) });

                    var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum value out of range."));

                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Switch(value, Expression.Throw(exception), cases.ToArray()));
                }
                else
                {
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(BinaryEnumSerializerBuilderCase)} can only be applied to {nameof(EnumResolution)}s."));
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryEnumSerializerBuilderCase)} can only be applied to {nameof(EnumSchema)}s."));
            }
        }
    }
}
