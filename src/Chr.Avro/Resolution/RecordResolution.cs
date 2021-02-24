namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents resolved information about a record-like <see cref="Type" />.
    /// </summary>
    public class RecordResolution : NamedTypeResolution
    {
        private ICollection<ConstructorResolution> constructors = default!;

        private ICollection<FieldResolution> fields = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="name">
        /// The resolved name.
        /// </param>
        /// <param name="namespace">
        /// The resolved namespace.
        /// </param>
        /// <param name="fields">
        /// The resolved <see cref="FieldResolution" />s. If no fields collection is supplied,
        /// <see cref="Fields" /> will be empty after initialization.
        /// </param>
        /// <param name="constructors">
        /// The resolved <see cref="ConstructorResolution" />s. If no constructors collection is
        /// supplied, <see cref="Constructors" /> will be empty after initialization.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public RecordResolution(Type type, IdentifierResolution name, IdentifierResolution? @namespace = null, IEnumerable<FieldResolution>? fields = null, IEnumerable<ConstructorResolution>? constructors = null, bool isNullable = false)
            : base(type, name, @namespace, isNullable)
        {
            Fields = fields?.ToList() ?? new List<FieldResolution>();
            Constructors = constructors?.ToList() ?? new List<ConstructorResolution>();
        }

        /// <summary>
        /// Gets or sets the resolved <see cref="ConstructorResolution" />s.
        /// </summary>
        public virtual ICollection<ConstructorResolution> Constructors
        {
            get
            {
                return constructors ?? throw new InvalidOperationException();
            }

            set
            {
                constructors = value ?? throw new ArgumentNullException(nameof(value), "Resolved constructor collection cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the resolved <see cref="FieldResolution" />s.
        /// </summary>
        public virtual ICollection<FieldResolution> Fields
        {
            get
            {
                return fields ?? throw new InvalidOperationException();
            }

            set
            {
                fields = value ?? throw new ArgumentNullException(nameof(value), "Resolved field collection cannot be null.");
            }
        }
    }
}
