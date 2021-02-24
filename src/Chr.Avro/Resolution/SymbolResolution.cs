namespace Chr.Avro.Resolution
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Represents resolved information about an enum symbol.
    /// </summary>
    public class SymbolResolution
    {
        private MemberInfo member = default!;

        private IdentifierResolution name = default!;

        private object value = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="SymbolResolution" /> class.
        /// </summary>
        /// <param name="member">
        /// The resolved static <see cref="MemberInfo" />.
        /// </param>
        /// <param name="name">
        /// The symbol name.
        /// </param>
        /// <param name="value">
        /// The raw symbol value.
        /// </param>
        public SymbolResolution(MemberInfo member, IdentifierResolution name, object value)
        {
            Member = member;
            Name = name;
            Value = value;
        }

        /// <summary>
        /// Gets or sets the resolved static <see cref="MemberInfo" />.
        /// </summary>
        public virtual MemberInfo Member
        {
            get
            {
                return member ?? throw new InvalidOperationException();
            }

            set
            {
                member = value ?? throw new ArgumentNullException(nameof(value), "Symbol reflection info cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the symbol name.
        /// </summary>
        public virtual IdentifierResolution Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }

            set
            {
                name = value ?? throw new ArgumentNullException(nameof(value), "Symbol name cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the raw symbol value.
        /// </summary>
        public virtual object Value
        {
            get
            {
                return value ?? throw new InvalidOperationException();
            }

            set
            {
                this.value = value ?? throw new ArgumentNullException(nameof(value), "Symbol value cannot be null.");
            }
        }
    }
}
