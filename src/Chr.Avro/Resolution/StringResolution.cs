namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a UTF-8 string-like <see cref="Type" />.
    /// </summary>
    public class StringResolution : TypeResolution
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StringResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public StringResolution(Type type, bool isNullable = false)
            : base(type, isNullable)
        {
        }
    }
}
