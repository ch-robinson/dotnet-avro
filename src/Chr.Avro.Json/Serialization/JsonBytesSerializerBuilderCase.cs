namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.Encodings.Web;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="BytesSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonBytesSerializerBuilderCase : BytesSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="BytesSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="BytesSchema" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
            if (schema is BytesSchema bytesSchema)
            {
                var bytes = Expression.Parameter(typeof(byte[]));
                var chars = Expression.Parameter(typeof(char[]));

                var copyTo = typeof(Array)
                    .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

                var encode = typeof(JsonEncodedText)
                    .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                try
                {
                    return JsonSerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { bytes, chars },
                            Expression.Assign(bytes, BuildConversion(value, typeof(byte[]))),
                            Expression.Assign(chars, Expression.NewArrayBounds(typeof(char), Expression.ArrayLength(bytes))),
                            Expression.Call(bytes, copyTo, chars, Expression.Constant(0)),
                            Expression.Call(
                                context.Writer,
                                writeString,
                                Expression.Call(
                                    null,
                                    encode,
                                    Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                    Expression.Constant(JsonEncoder.Bytes)))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"Failed to map {bytesSchema} to {resolution.Type}.", exception);
                }
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonBytesSerializerBuilderCase)} can only be applied to {nameof(BytesSchema)}s."));
            }
        }
    }
}
