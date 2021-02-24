namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match
    /// <see cref="MicrosecondTimestampLogicalType" /> or <see cref="MillisecondTimestampLogicalType" />.
    /// </summary>
    public class TimestampDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <summary>
        /// A <see cref="DateTime" /> representing the Unix epoch (1970-01-01T00:00:00.000Z).
        /// </summary>
        protected static readonly DateTime Epoch = new (1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }
}
