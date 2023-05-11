namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="BytesSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryBytesSerializerBuilderCase : BytesSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="BytesSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="BytesSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is BytesSchema bytesSchema)
            {
                var writeBytes = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteBytes), new[] { typeof(byte[]) });

                try
                {
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeBytes, BuildConversion(value, typeof(byte[]))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {bytesSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryBytesSerializerBuilderCase)} can only be applied to {nameof(BytesSchema)}s."));
            }
        }
    }
}
