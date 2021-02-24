namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a .NET <see cref="System.Type" />.
    /// </summary>
    public abstract class TypeResolution
    {
        private Type type = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="TypeResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        public TypeResolution(Type type, bool isNullable = false)
        {
            IsNullable = isNullable;
            Type = type;
        }

        /// <summary>
        /// Gets or sets a value indicating whether <see cref="Type" /> can have a <c>null</c> value.
        /// </summary>
        public virtual bool IsNullable { get; set; }

        /// <summary>
        /// Gets or sets the resolved <see cref="System.Type" />.
        /// </summary>
        public virtual Type Type
        {
            get
            {
                return type ?? throw new InvalidOperationException();
            }

            set
            {
                type = value ?? throw new ArgumentNullException(nameof(value), "Resolved type cannot be null.");
            }
        }
    }
}
