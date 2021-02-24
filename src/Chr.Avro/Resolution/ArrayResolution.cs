namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents resolved information about an array-like <see cref="Type" />.
    /// </summary>
    public class ArrayResolution : TypeResolution
    {
        private ICollection<ConstructorResolution> constructors = default!;

        private Type itemType = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArrayResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="itemType">
        /// The resolved item <see cref="Type" />.
        /// </param>
        /// <param name="constructors">
        /// The resolved <see cref="ConstructorResolution" />s.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public ArrayResolution(Type type, Type itemType, IEnumerable<ConstructorResolution>? constructors = null, bool isNullable = false)
            : base(type, isNullable)
        {
            Constructors = constructors?.ToList() ?? new List<ConstructorResolution>();
            ItemType = itemType;
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
        /// Gets or sets the resolved item <see cref="Type" />.
        /// </summary>
        public virtual Type ItemType
        {
            get
            {
                return itemType ?? throw new InvalidOperationException();
            }

            set
            {
                itemType = value ?? throw new ArgumentNullException(nameof(value), "Resolved item type cannot be null.");
            }
        }
    }
}
