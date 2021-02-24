namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a URI-like <see cref="Type" />.
    /// </summary>
    public class UriResolution : TypeResolution
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UriResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public UriResolution(Type type, bool isNullable = false)
            : base(type, isNullable)
        {
        }
    }
}
