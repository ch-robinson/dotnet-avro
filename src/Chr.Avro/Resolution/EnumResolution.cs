namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents resolved information about an enum-like <see cref="Type" />.
    /// </summary>
    public class EnumResolution : NamedTypeResolution
    {
        private ICollection<SymbolResolution> symbols = default!;

        private Type underlyingType = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="underlyingType">
        /// The resolved underlying integral <see cref="Type" />.
        /// </param>
        /// <param name="name">
        /// The resolved name.
        /// </param>
        /// <param name="namespace">
        /// The resolved namespace.
        /// </param>
        /// <param name="isFlagEnum">
        /// Whether <paramref name="type" /> is a bit flag enum.
        /// </param>
        /// <param name="symbols">
        /// The resolved <see cref="SymbolResolution" />s. If no symbol collection is supplied,
        /// <see cref="Symbols" /> will be empty after initialization.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public EnumResolution(Type type, Type underlyingType, IdentifierResolution name, IdentifierResolution? @namespace = null, bool isFlagEnum = false, IEnumerable<SymbolResolution>? symbols = null, bool isNullable = false)
            : base(type, name, @namespace, isNullable)
        {
            IsFlagEnum = isFlagEnum;
            Symbols = symbols?.ToList() ?? new List<SymbolResolution>();
            UnderlyingType = underlyingType;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the <see cref="Type" /> is a bit flag enum.
        /// </summary>
        public virtual bool IsFlagEnum { get; set; }

        /// <summary>
        /// Gets or sets the resolved underlying integral <see cref="Type" />.
        /// </summary>
        public virtual Type UnderlyingType
        {
            get
            {
                return underlyingType ?? throw new InvalidOperationException();
            }

            set
            {
                underlyingType = value ?? throw new ArgumentNullException(nameof(value), "Resolved underlying type cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the resolved <see cref="SymbolResolution" />s.
        /// </summary>
        public virtual ICollection<SymbolResolution> Symbols
        {
            get
            {
                return symbols ?? throw new InvalidOperationException();
            }

            set
            {
                symbols = value ?? throw new ArgumentNullException(nameof(value), "Resolved symbol collection cannot be null.");
            }
        }
    }
}
