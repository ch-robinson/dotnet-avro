namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

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
        /// Thrown when <see cref="decimal" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var scale = decimalLogicalType.Scale;

                Expression expression;

                var decodeDecimal = typeof(BinaryDecimalCodec)
                    .GetMethod(nameof(BinaryDecimalCodec.DecodeDecimal), BindingFlags.Static | BindingFlags.Public)!;

                // figure out the size:
                if (schema is BytesSchema)
                {
                    var readBytes = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadBytesSpan), Type.EmptyTypes)!;

                    expression = Expression.Call(
                        null,
                        decodeDecimal,
                        Expression.Call(context.Reader, readBytes),
                        Expression.Constant(scale));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var readFixed = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadFixedSpan), new[] { typeof(int) })!;

                    expression = Expression.Call(
                        null,
                        decodeDecimal,
                        Expression.Call(context.Reader, readFixed, Expression.Constant(fixedSchema.Size)),
                        Expression.Constant(scale));
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(expression, type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDecimalDeserializerBuilderCase)} can only be applied schemas with a {nameof(DecimalLogicalType)}."));
            }
        }
    }
}
