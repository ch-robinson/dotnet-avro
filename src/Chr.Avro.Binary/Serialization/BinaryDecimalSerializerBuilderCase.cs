namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
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
                var precision = Expression.Constant(decimalLogicalType.Precision);
                var scale = Expression.Constant(decimalLogicalType.Scale);

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(decimal));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                // figure out how to write:
                if (schema is BytesSchema)
                {
                    var writeBytes = typeof(BinaryDecimalCodec)
                        .GetMethod(nameof(BinaryDecimalCodec.WriteDecimalBytes), BindingFlags.Static | BindingFlags.Public)!;

                    expression = Expression.Block(
                        expression,
                        Expression.Call(writeBytes, context.Writer, expression, precision, scale));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var writeFixed = typeof(BinaryDecimalCodec)
                        .GetMethod(nameof(BinaryDecimalCodec.WriteDecimalFixed), BindingFlags.Static | BindingFlags.Public)!;

                    expression = Expression.Block(
                        expression,
                        Expression.Call(writeFixed, context.Writer, expression, precision, scale, Expression.Constant(fixedSchema)));
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
