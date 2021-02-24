namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents resolved information about a map-like <see cref="Type" />.
    /// </summary>
    public class MapResolution : TypeResolution
    {
        private ICollection<ConstructorResolution> constructors = default!;

        private Type keyType = default!;

        private Type valueType = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="MapResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="keyType">
        /// The resolved key <see cref="Type" />.
        /// </param>
        /// <param name="valueType">
        /// The resolved value <see cref="Type" />.
        /// </param>
        /// <param name="constructors">
        /// The resolved <see cref="ConstructorResolution" />s.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public MapResolution(Type type, Type keyType, Type valueType, IEnumerable<ConstructorResolution>? constructors = null, bool isNullable = false)
            : base(type, isNullable)
        {
            Constructors = constructors?.ToList() ?? new List<ConstructorResolution>();
            KeyType = keyType;
            ValueType = valueType;
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
        /// Gets or sets the resolved key <see cref="Type" />.
        /// </summary>
        public virtual Type KeyType
        {
            get
            {
                return keyType ?? throw new InvalidOperationException();
            }

            set
            {
                keyType = value ?? throw new ArgumentNullException(nameof(value), "Resolved key type cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the resolved value <see cref="Type" />.
        /// </summary>
        public virtual Type ValueType
        {
            get
            {
                return valueType ?? throw new InvalidOperationException();
            }

            set
            {
                valueType = value ?? throw new ArgumentNullException(nameof(value), "Resolved value type cannot be null.");
            }
        }
    }
}
