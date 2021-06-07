namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Numerics;
    using System.Text;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="DecimalLogicalType" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonDecimalDeserializerBuilderCase : DecimalDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> for a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DecimalLogicalType" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="BytesSchema" /> or a
        /// <see cref="FixedSchema "/>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="decimal" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;

                var bytes = Expression.Variable(typeof(byte[]));

                var tokenType = typeof(Utf8JsonReader)
                    .GetProperty(nameof(Utf8JsonReader.TokenType));

                var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                var getString = typeof(Utf8JsonReader)
                    .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                var getBytes = typeof(Encoding)
                    .GetMethod(nameof(Encoding.GetBytes), new[] { typeof(string) });

                var getUnexpectedSizeException = typeof(JsonExceptionHelper)
                    .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedSizeException));

                Expression expression;

                if (schema is BytesSchema)
                {
                    expression = Expression.Block(
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.Property(context.Reader, tokenType),
                                Expression.Constant(JsonTokenType.String)),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    getUnexpectedTokenException,
                                    context.Reader,
                                    Expression.Constant(new[] { JsonTokenType.String })))),
                        Expression.Assign(
                            bytes,
                            Expression.Call(
                                Expression.Constant(JsonEncoding.Bytes),
                                getBytes,
                                Expression.Call(context.Reader, getString))),
                        bytes);
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    expression = Expression.Block(
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.Property(context.Reader, tokenType),
                                Expression.Constant(JsonTokenType.String)),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    getUnexpectedTokenException,
                                    context.Reader,
                                    Expression.Constant(new[] { JsonTokenType.String })))),
                        Expression.Assign(
                            bytes,
                            Expression.Call(
                                Expression.Constant(JsonEncoding.Bytes),
                                getBytes,
                                Expression.Call(context.Reader, getString))),
                        Expression.IfThen(
                            Expression.NotEqual(
                                Expression.ArrayLength(bytes),
                                Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(
                                Expression.Call(
                                    null,
                                    getUnexpectedSizeException,
                                    context.Reader,
                                    Expression.Constant(fixedSchema.Size),
                                    Expression.ArrayLength(bytes)))),
                        bytes);
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                // declare variables for in-place transformation:
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
                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(expression, type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDecimalDeserializerBuilderCase)} can only be applied schemas with a {nameof(DecimalLogicalType)}."));
            }
        }
    }
}
