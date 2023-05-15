namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Numerics;
    using System.Text.Encodings.Web;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="DecimalLogicalType" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonDecimalSerializerBuilderCase : DecimalSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DecimalLogicalType" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
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
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
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
                var chars = Expression.Parameter(typeof(char[]));

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(decimal) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                var toByteArray = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

                var copyTo = typeof(Array)
                    .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

                // var fraction = new BigInteger(...) * BigInteger.Pow(10, scale);
                // var whole = new BigInteger((... % 1m) * (decimal)Math.Pow(10, scale));
                // var bytes = (fraction + whole).ToByteArray();
                //
                // // BigInteger is little-endian, so reverse:
                // Array.Reverse(bytes);
                //
                // var chars = new char[bytes.Length];
                // bytes.CopyTo(chars, 0);
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
                    Expression.Assign(chars, Expression.NewArrayBounds(typeof(char), Expression.ArrayLength(bytes))),
                    Expression.Call(bytes, copyTo, chars, Expression.Constant(0)));

                var encode = typeof(JsonEncodedText)
                    .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                // figure out how to write:
                if (schema is BytesSchema)
                {
                    expression = Expression.Block(
                        new[] { bytes, chars },
                        expression,
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                null,
                                encode,
                                Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                Expression.Constant(JsonEncoder.Bytes))));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    expression = Expression.Block(
                        new[] { bytes, chars },
                        expression,
                        Expression.IfThen(
                            Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema.Name} (size {fixedSchema.Size}) and decimal with precision {precision} and scale {scale}.")))),
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                null,
                                encode,
                                Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                Expression.Constant(JsonEncoder.Bytes))));
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                return JsonSerializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDecimalSerializerBuilderCase)} can only be applied schemas with a {nameof(DecimalLogicalType)}."));
            }
        }
    }
}
