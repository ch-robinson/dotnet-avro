namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match
    /// <see cref="TimestampLogicalType" />s.
    /// </summary>
    public class TimestampDeserializerBuilderCase : DeserializerBuilderCase
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
        /// Builds an <see cref="Expression" /> representing the conversion of a Unix timestamp to
        /// ticks (1 tick = 100 ns).
        /// </summary>
        /// <param name="value">
        /// An <see cref="Expression" /> with type <see cref="long" /> representing a Unix timestamp
        /// value.
        /// </param>
        /// <param name="schema">
        /// The schema of the timestamp value.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing <paramref name="value" /> as the number of
        /// ticks from <see cref="Epoch" /> .
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="LongSchema" /> or when
        /// <paramref name="schema" /> does not have a <see cref="MicrosecondTimestampLogicalType" />,
        /// <see cref="MillisecondTimestampLogicalType" />, or <see cref="NanosecondTimestampLogicalType" />.
        /// </exception>
        protected virtual Expression BuildTimestampToTicks(Expression value, Schema schema)
        {
            return schema.LogicalType switch
            {
                MicrosecondTimestampLogicalType =>
                    Expression.MultiplyChecked(
                        value,
#if NET8_0_OR_GREATER
                        Expression.Constant(TimeSpan.TicksPerMicrosecond)),
#else
                        Expression.Constant(10L)),
#endif
                MillisecondTimestampLogicalType =>
                    Expression.MultiplyChecked(
                        value,
                        Expression.Constant(TimeSpan.TicksPerMillisecond)),
                NanosecondTimestampLogicalType =>
                    Expression.Divide(
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
