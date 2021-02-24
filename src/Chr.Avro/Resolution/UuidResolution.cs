namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a UUID-like <see cref="Type" />.
    /// </summary>
    /// <remarks>
    /// See https://stackoverflow.com/a/6953207 for a summary of UUID variants/versions.
    /// </remarks>
    public class UuidResolution : TypeResolution
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="UuidResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="variant">
        /// The RFC 4122 variant of <paramref name="type" />.
        /// </param>
        /// <param name="version">
        /// The RFC 4122 sub-variant of <paramref name="type" />.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public UuidResolution(Type type, int variant, int? version = null, bool isNullable = false)
            : base(type, isNullable)
        {
            Variant = variant;
            Version = version;
        }

        /// <summary>
        /// Gets or sets the RFC 4122 variant of the <see cref="Type" />.
        /// </summary>
        public virtual int Variant { get; set; }

        /// <summary>
        /// Gets or sets the RFC 4122 sub-variant of the <see cref="Type" />.
        /// </summary>
        public virtual int? Version { get; set; }
    }
}
