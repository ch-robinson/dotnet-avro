namespace Chr.Avro.Abstract
{
    /// <summary>
    /// The behavior of how temporal types like DateTime are being serialized.
    /// Options include Iso8601 (string based), EpochMicroseconds and EpochMilliseconds (long based)
    /// </summary>
    public enum TemporalBehavior
    {
        /// <summary>
        /// Serialize using a string (https://en.wikipedia.org/wiki/ISO_8601)
        /// </summary>
        Iso8601,
        /// <summary>
        /// Serialize using a long (microseconds since 1970-01-01 00:00:00 UTC)
        /// </summary>
        EpochMicroseconds,
        /// <summary>
        /// Serialize using a long (milliseconds since 1970-01-01 00:00:00 UTC)
        /// </summary>
        EpochMilliseconds
    }
}
