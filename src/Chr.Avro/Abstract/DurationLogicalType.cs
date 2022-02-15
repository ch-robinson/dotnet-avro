namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Represents an Avro logical type defining an amount of time consisting of month, day, and
    /// millisecond components.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Duration">Avro spec</a> for
    /// details.
    /// </remarks>
    public class DurationLogicalType : LogicalType
    {
        /// <summary>
        /// The size of a duration (three 32-bit unsigned integers).
        /// </summary>
        public const int DurationSize = 12;
    }
}
