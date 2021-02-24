namespace Chr.Avro.Resolution
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Represents resolved information about a field or property.
    /// </summary>
    public class FieldResolution
    {
        private MemberInfo member = default!;

        private IdentifierResolution name = default!;

        private Type type = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="FieldResolution" /> class.
        /// </summary>
        /// <param name="member">
        /// The resolved <see cref="MemberInfo" />.
        /// </param>
        /// <param name="type">
        /// The field or property <see cref="Type" />.
        /// </param>
        /// <param name="name">
        /// The field or property name.
        /// </param>
        public FieldResolution(MemberInfo member, Type type, IdentifierResolution name)
        {
            Member = member;
            Type = type;
            Name = name;
        }

        /// <summary>
        /// Gets or sets the resolved <see cref="MemberInfo" />.
        /// </summary>
        public virtual MemberInfo Member
        {
            get
            {
                return member ?? throw new InvalidOperationException();
            }

            set
            {
                member = value ?? throw new ArgumentNullException(nameof(value), "Field reflection info cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the field or property name.
        /// </summary>
        public virtual IdentifierResolution Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }

            set
            {
                name = value ?? throw new ArgumentNullException(nameof(value), "Field name cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the field or property <see cref="Type" />.
        /// </summary>
        public virtual Type Type
        {
            get
            {
                return type ?? throw new InvalidOperationException();
            }

            set
            {
                type = value ?? throw new ArgumentNullException(nameof(value), "Field type cannot be null.");
            }
        }
    }
}
