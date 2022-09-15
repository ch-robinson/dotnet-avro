namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Encodings.Web;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="DurationLogicalType" />
    /// and attempts to map it to <see cref="TimeSpan" />.
    /// </summary>
    public class JsonDurationSerializerBuilderCase : DurationSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="DurationLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DurationLogicalType" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="FixedSchema" /> with size
        /// <c>12</c>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="TimeSpan" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema.LogicalType is DurationLogicalType)
            {
                if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var chars = Expression.Parameter(typeof(char[]));

                var getBytes = typeof(BitConverter)
                    .GetMethod(nameof(BitConverter.GetBytes), new[] { typeof(uint) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { getBytes.ReturnType });

                var copyTo = typeof(Array)
                    .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

                Expression Write(Expression value, Expression offset)
                {
                    Expression component = Expression.Call(null, getBytes, value);

                    if (!BitConverter.IsLittleEndian)
                    {
                        var buffer = Expression.Variable(component.Type);

                        component = Expression.Block(
                            new[] { buffer },
                            Expression.Assign(buffer, component),
                            Expression.Call(null, reverse, buffer),
                            buffer);
                    }

                    return Expression.Call(component, copyTo, chars, offset);
                }

                var totalDays = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalDays));
                var totalMs = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalMilliseconds));

                var encode = typeof(JsonEncodedText)
                    .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                return JsonSerializerBuilderCaseResult.FromExpression(
                    Expression.Block(
                        new[] { chars },
                        Expression.Assign(
                            chars,
                            Expression.NewArrayBounds(typeof(char), Expression.Constant(12))),
                        Write(
                            Expression.ConvertChecked(Expression.Property(value, totalDays), typeof(uint)),
                            Expression.Constant(4)),
                        Write(
                            Expression.ConvertChecked(
                                Expression.Subtract(
                                    Expression.ConvertChecked(Expression.Property(value, totalMs), typeof(ulong)),
                                    Expression.Multiply(
                                        Expression.ConvertChecked(Expression.Property(value, totalDays), typeof(ulong)),
                                        Expression.Constant(86400000UL))),
                                typeof(uint)),
                            Expression.Constant(8)),
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                null,
                                encode,
                                Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                Expression.Constant(JsonEncoder.Bytes)))));
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonDurationSerializerBuilderCase)} can only be applied schemas with a {nameof(DurationLogicalType)}."));
            }
        }
    }
}
