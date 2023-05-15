namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="DurationLogicalType" />
    /// and attempts to map it to <see cref="TimeSpan" />.
    /// </summary>
    public class BinaryDurationSerializerBuilderCase : DurationSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="DurationLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DurationLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
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
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema.LogicalType is DurationLogicalType)
            {
                if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                {
                    throw new UnsupportedSchemaException(schema);
                }

                Expression Write(Expression value)
                {
                    var getBytes = typeof(BitConverter)
                        .GetMethod(nameof(BitConverter.GetBytes), new[] { value.Type });

                    Expression bytes = Expression.Call(null, getBytes, value);

                    if (!BitConverter.IsLittleEndian)
                    {
                        var buffer = Expression.Variable(bytes.Type);
                        var reverse = typeof(Array)
                            .GetMethod(nameof(Array.Reverse), new[] { bytes.Type });

                        bytes = Expression.Block(
                            new[] { buffer },
                            Expression.Assign(buffer, bytes),
                            Expression.Call(null, reverse, buffer),
                            buffer);
                    }

                    var writeFixed = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteFixed), new[] { bytes.Type });

                    return Expression.Call(context.Writer, writeFixed, bytes);
                }

                var totalDays = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalDays));
                var totalMs = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalMilliseconds));

                return BinarySerializerBuilderCaseResult.FromExpression(
                    Expression.Block(
                        Write(Expression.Constant(0U)),
                        Write(
                            Expression.ConvertChecked(Expression.Property(value, totalDays), typeof(uint))),
                        Write(
                            Expression.ConvertChecked(
                                Expression.Subtract(
                                    Expression.ConvertChecked(Expression.Property(value, totalMs), typeof(ulong)),
                                    Expression.Multiply(
                                        Expression.ConvertChecked(Expression.Property(value, totalDays), typeof(ulong)),
                                        Expression.Constant(86400000UL))),
                                typeof(uint)))));
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDurationSerializerBuilderCase)} can only be applied schemas with a {nameof(DurationLogicalType)}."));
            }
        }
    }
}
