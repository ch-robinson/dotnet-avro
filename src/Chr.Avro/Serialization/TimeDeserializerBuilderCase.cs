#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match
    /// <see cref="MicrosecondTimeLogicalType" /> or <see cref="MillisecondTimeLogicalType" />.
    /// </summary>
    public class TimeDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <summary>
        /// A <see cref="TimeOnly" /> representing midnight (00:00:00.000).
        /// </summary>
        protected static readonly TimeOnly Midnight = new(0);
    }
}
#endif
