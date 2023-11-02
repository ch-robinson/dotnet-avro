#if NET6_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match
    /// <see cref="DateLogicalType" />.
    /// </summary>
    public class DateDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <summary>
        /// A <see cref="DateOnly" /> representing the Unix epoch (1970-01-01).
        /// </summary>
        protected static readonly DateOnly Epoch = new(1970, 1, 1);
    }
}
#endif
