namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a named <see cref="Type" /> (i.e., a class, struct,
    /// interface, or enum).
    /// </summary>
    public abstract class NamedTypeResolution : TypeResolution
    {
        private IdentifierResolution name = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="NamedTypeResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The named type.
        /// </param>
        /// <param name="name">
        /// The type name.
        /// </param>
        /// <param name="namespace">
        /// The type namespace.
        /// </param>
        /// <param name="isNullable">
        /// Whether the named type can have a null value.
        /// </param>
        public NamedTypeResolution(Type type, IdentifierResolution name, IdentifierResolution? @namespace = null, bool isNullable = false)
            : base(type, isNullable)
        {
            Name = name;
            Namespace = @namespace;
        }

        /// <summary>
        /// Gets or sets the resolved name.
        /// </summary>
        public virtual IdentifierResolution Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }

            set
            {
                name = value ?? throw new ArgumentNullException(nameof(value), "Resolved name cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the resolved namespace.
        /// </summary>
        public virtual IdentifierResolution? Namespace { get; set; }
    }
}
