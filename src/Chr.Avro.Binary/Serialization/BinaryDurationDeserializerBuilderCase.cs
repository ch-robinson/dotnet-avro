namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="DurationLogicalType" />
    /// and attempts to map it to <see cref="TimeSpan" />.
    /// </summary>
    public class BinaryDurationDeserializerBuilderCase : DurationDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="DurationLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DurationLogicalType" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="FixedSchema" /> with size
        /// <c>12</c>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="TimeSpan" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema.LogicalType is DurationLogicalType)
            {
                if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == DurationLogicalType.DurationSize))
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var readFixed = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadFixed), new[] { typeof(int) });

                Expression read = Expression.Call(context.Reader, readFixed, Expression.Constant(4));

                if (!BitConverter.IsLittleEndian)
                {
                    var buffer = Expression.Variable(read.Type);
                    var reverse = typeof(Array)
                        .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                    read = Expression.Block(
                        new[] { buffer },
                        Expression.Assign(buffer, read),
                        Expression.Call(null, reverse, Expression.Convert(buffer, typeof(Array))),
                        buffer);
                }

                var toUInt32 = typeof(BitConverter)
                    .GetMethod(nameof(BitConverter.ToUInt32), new[] { typeof(byte[]), typeof(int) });

                read = Expression.ConvertChecked(
                    Expression.Call(null, toUInt32, read, Expression.Constant(0)),
                    typeof(long));

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                var timeSpanConstructor = typeof(TimeSpan)
                    .GetConstructor(new[] { typeof(long) });

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.NotEqual(read, Expression.Constant(0L)),
                                    Expression.Throw(
                                        Expression.New(
                                            exceptionConstructor,
                                            Expression.Constant($"Durations containing months cannot be accurately deserialized to a {nameof(TimeSpan)}.")))),
                                Expression.New(
                                    timeSpanConstructor,
                                    Expression.AddChecked(
                                        Expression.MultiplyChecked(read, Expression.Constant(TimeSpan.TicksPerDay)),
                                        Expression.MultiplyChecked(read, Expression.Constant(TimeSpan.TicksPerMillisecond))))),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDurationDeserializerBuilderCase)} can only be applied schemas with a {nameof(DurationLogicalType)}."));
            }
        }
    }
}
