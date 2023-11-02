#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="TimeLogicalType" />
    /// and attempts to map it to <see cref="TimeOnly" />.
    /// </summary>
    public class BinaryTimeDeserializerBuilderCase : TimeDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="TimeLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimeLogicalType" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> with a
        /// <see cref="MicrosecondTimeLogicalType" /> or when <paramref name="schema" /> is not an
        /// <see cref="IntSchema" /> with a <see cref="MillisecondTimeLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="TimeOnly" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema.LogicalType is TimeLogicalType)
            {
                if (schema.LogicalType is MicrosecondTimeLogicalType && schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(MicrosecondTimeLogicalType)} deserializers can only be built for {nameof(LongSchema)}s.");
                }

                if (schema.LogicalType is MillisecondTimeLogicalType && schema is not IntSchema)
                {
                    throw new UnsupportedSchemaException(schema, $"{nameof(MillisecondTimeLogicalType)} deserializers can only be built for {nameof(IntSchema)}s.");
                }

                var factor = schema.LogicalType switch
                {
                    MicrosecondTimeLogicalType => TimeSpan.TicksPerMillisecond / 1000,
                    MillisecondTimeLogicalType => TimeSpan.TicksPerMillisecond,
                    _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimeLogicalType)}."),
                };

                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, readInteger);

                var timeSpanConstructor = typeof(TimeSpan)
                    .GetConstructor(new[] { typeof(long) });

                var add = typeof(TimeOnly)
                    .GetMethod(nameof(TimeOnly.Add), new[] { typeof(TimeSpan) });

                try
                {
                    // return Midnight.Add(new TimeSpan(value * factor));
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(
                            Expression.Call(
                                Expression.Constant(Midnight),
                                add,
                                Expression.New(
                                    timeSpanConstructor,
                                    Expression.Multiply(expression, Expression.Constant(factor)))),
                            type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryTimeDeserializerBuilderCase)} can only be applied to schemas with a {nameof(TimeLogicalType)}."));
            }
        }
    }
}
#endif
