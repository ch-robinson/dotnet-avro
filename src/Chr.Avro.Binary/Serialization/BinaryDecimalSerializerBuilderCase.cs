namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Numerics;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="DecimalLogicalType" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryDecimalSerializerBuilderCase : DecimalSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DecimalLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="BytesSchema" /> or a
        /// <see cref="FixedSchema "/>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="decimal" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(decimal));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                // declare variables for in-place transformation:
                var bytes = Expression.Variable(typeof(byte[]));

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(decimal) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                var toByteArray = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

                // var fraction = new BigInteger(...) * BigInteger.Pow(10, scale);
                // var whole = new BigInteger((... % 1m) * (decimal)Math.Pow(10, scale));
                // var bytes = (fraction + whole).ToByteArray();
                //
                // // BigInteger is little-endian, so reverse:
                // Array.Reverse(bytes);
                //
                // return bytes;
                expression = Expression.Block(
                    Expression.Assign(
                        bytes,
                        Expression.Call(
                            Expression.Add(
                                Expression.Multiply(
                                    Expression.New(
                                        integerConstructor,
                                        expression),
                                    Expression.Constant(BigInteger.Pow(10, scale))),
                                Expression.New(
                                    integerConstructor,
                                    Expression.Multiply(
                                        Expression.Modulo(expression, Expression.Constant(1m)),
                                        Expression.Constant((decimal)Math.Pow(10, scale))))),
                            toByteArray)),
                    Expression.Call(null, reverse, bytes),
                    bytes);

                // figure out how to write:
                if (schema is BytesSchema)
                {
                    var writeBytes = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteBytes), new[] { typeof(byte[]) });

                    expression = Expression.Block(
                        new[] { bytes },
                        expression,
                        Expression.Call(context.Writer, writeBytes, bytes));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    var writeFixed = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteFixed), new[] { typeof(byte[]) });

                    expression = Expression.Block(
                        new[] { bytes },
                        expression,
                        Expression.IfThen(
                            Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema} (size {fixedSchema.Size}) and decimal with precision {precision} and scale {scale}.")))),
                        Expression.Call(context.Writer, writeFixed, bytes));
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                return BinarySerializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDecimalSerializerBuilderCase)} can only be applied schemas with a {nameof(DecimalLogicalType)}."));
            }
        }
    }
}
