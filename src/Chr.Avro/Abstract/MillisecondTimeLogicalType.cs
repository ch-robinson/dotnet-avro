namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Represents an Avro logical type defining a time of day (with no reference to a particular
    /// time zone) in milliseconds after midnight.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Time+(millisecond+precision)">Avro spec</a>
    /// for details.
    /// </remarks>
    public class MillisecondTimeLogicalType : TimeLogicalType
    {
    }
}
