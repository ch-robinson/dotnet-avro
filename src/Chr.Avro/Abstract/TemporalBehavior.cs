namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Options for building schemas for temporal types (timestamps, durations, etc.).
    /// </summary>
    public enum TemporalBehavior
    {
        /// <summary>
        /// Prefer to represent temporal types as ISO 8601 strings (https://en.wikipedia.org/wiki/ISO_8601).
        /// </summary>
        Iso8601,

        /// <summary>
        /// Prefer to represent temporal types as microseconds since epoch (1970-01-01 00:00:00 UTC).
        /// </summary>
        EpochMicroseconds,

        /// <summary>
        /// Prefer to represent temporal types as milliseconds since epoch (1970-01-01 00:00:00 UTC).
        /// </summary>
        EpochMilliseconds,
    }
}
