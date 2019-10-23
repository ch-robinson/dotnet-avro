namespace Chr.Avro.Abstract
{
    /// <summary>
    /// The behavior of how temporal types like DateTime are being serialized.
    /// Options include Iso8601 (string based), EpochMicroseconds and EpochMilliseconds (long based)
    /// </summary>
    public enum TemporalBehavior
    {
        /// <summary>
        /// Serialize using a string 
        /// </summary>
        Iso8601,
        /// <summary>
        /// Serialize using a long (epoch)
        /// </summary>
        EpochMicroseconds,
        /// <summary>
        /// Serialize using a long (epoch)
        /// </summary>
        EpochMilliseconds
    }
}
