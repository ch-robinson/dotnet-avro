namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a byte array-like <see cref="Type" />.
    /// </summary>
    public class ByteArrayResolution : TypeResolution
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ByteArrayResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public ByteArrayResolution(Type type, bool isNullable = false)
            : base(type, isNullable)
        {
        }
    }
}
