namespace Chr.Avro.NodaTimeExample.Infrastructure
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    public class NodaTimeAsTimestampSerializerBuilderCase : BinaryTimestampSerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="TimestampLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="TimestampLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />
        /// or a <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="DateTimeOffset" />.
        /// </exception>
        /// <inheritdoc />
        public override BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema.LogicalType is TimestampLogicalType)
            {
                if (schema is not LongSchema)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var nodaTimeType = typeof(NodaTime.Instant);

                // If NodaTime.Instant just change the Expression to a DateTimeOffset
                if (type == nodaTimeType)
                {
                    var methodInfo = nodaTimeType.GetMethod(nameof(NodaTime.Instant.ToDateTimeOffset), Array.Empty<Type>())
                        ?? throw new NullReferenceException("Tried to find the method 'NodaTime.Instant.ToDateTimeOffset()', but it seem to have disappeared.");

                    // Code: value = value.ToDateTimeOffset();
                    // The base class implementation will fruther handle the value as normal DateTimeOffset.
                    value = Expression.Call(value, methodInfo);
                }

                return base.BuildExpression(value, type, schema, context);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(NodaTimeAsTimestampSerializerBuilderCase)} can only be applied schemas with a {nameof(TimestampLogicalType)}."));
            }
        }
    }
}
