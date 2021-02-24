namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Numerics;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="DecimalLogicalType" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryDecimalDeserializerBuilderCase : DecimalDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DecimalLogicalType" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="BytesSchema" /> or a
        /// <see cref="FixedSchema "/>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="decimal" /> cannot be converted to the resolved <see cref="Type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;

                Expression expression;

                // figure out the size:
                if (schema is BytesSchema)
                {
                    var readBytes = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadBytes), Type.EmptyTypes);

                    expression = Expression.Call(context.Reader, readBytes);
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var readFixed = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadFixed), new[] { typeof(long) });

                    expression = Expression.Call(context.Reader, readFixed, Expression.Constant((long)fixedSchema.Size));
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                // declare variables for in-place transformation:
                var bytes = Expression.Variable(typeof(byte[]));
                var remainder = Expression.Variable(typeof(BigInteger));

                var divide = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.DivRem), new[] { typeof(BigInteger), typeof(BigInteger), typeof(BigInteger).MakeByRefType() });

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(byte[]) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                // var bytes = ...;
                //
                // // BigInteger is little-endian, so reverse:
                // Array.Reverse(bytes);
                //
                // var whole = BigInteger.DivRem(new BigInteger(bytes), BigInteger.Pow(10, scale), out var remainder);
                // var fraction = (decimal)remainder / (decimal)Math.Pow(10, scale);
                //
                // return whole + fraction;
                expression = Expression.Block(
                    new[] { bytes, remainder },
                    Expression.Assign(bytes, expression),
                    Expression.Call(null, reverse, bytes),
                    Expression.Add(
                        Expression.ConvertChecked(
                            Expression.Call(
                                null,
                                divide,
                                Expression.New(integerConstructor, bytes),
                                Expression.Constant(BigInteger.Pow(10, scale)),
                                remainder),
                            typeof(decimal)),
                        Expression.Divide(
                            Expression.ConvertChecked(remainder, typeof(decimal)),
                            Expression.Constant((decimal)Math.Pow(10, scale)))));

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(expression, resolution.Type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"Failed to map {schema} to {resolution.Type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDecimalDeserializerBuilderCase)} can only be applied schemas with a {nameof(DecimalLogicalType)}."));
            }
        }
    }
}
