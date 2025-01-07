namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match
    /// <see cref="TimestampLogicalType" />s.
    /// </summary>
    public class TimestampSerializerBuilderCase : SerializerBuilderCase
    {
        /// <summary>
        /// A <see cref="DateTime" /> representing the Unix epoch (1970-01-01T00:00:00.000Z).
        /// </summary>
#if NET6_0_OR_GREATER
        protected static readonly DateTime Epoch = DateTime.UnixEpoch;
#else
        protected static readonly DateTime Epoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
#endif

        /// <summary>
        /// Builds an <see cref="Expression" /> representing the conversion of ticks (1 tick = 100
        /// ns) to a Unix timestamp.
        /// </summary>
        /// <param name="value">
        /// An <see cref="Expression" /> with type <see cref="long" /> representing a number of
        /// ticks.
        /// </param>
        /// <param name="schema">
        /// The schema of the timestamp value.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing <paramref name="value" /> as a Unix
        /// timestamp.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />,
        /// <see cref="MillisecondTimestampLogicalType" />, or <see cref="NanosecondTimestampLogicalType" />.
        /// </exception>
        protected virtual Expression BuildTicksToTimestamp(Expression value, Schema schema)
        {
            return schema.LogicalType switch
            {
                MicrosecondTimestampLogicalType =>
                    Expression.Divide(
                        value,
#if NET8_0_OR_GREATER
                        Expression.Constant(TimeSpan.TicksPerMicrosecond)),
#else
                        Expression.Constant(10L)),
#endif
                MillisecondTimestampLogicalType =>
                    Expression.Divide(
                        value,
                        Expression.Constant(TimeSpan.TicksPerMillisecond)),
                NanosecondTimestampLogicalType =>
                    Expression.MultiplyChecked(
                        value,
#if NET8_0_OR_GREATER
                        Expression.Constant(TimeSpan.NanosecondsPerTick)),
#else
                        Expression.Constant(100L)),
#endif
                _ => throw new UnsupportedSchemaException(schema, $"{schema.LogicalType} is not a supported {nameof(TimestampLogicalType)}."),
            };
        }
    }
}
