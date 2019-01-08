using System;
using System.Reflection;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// Contains resolved information about a field or property.
    /// </summary>
    public class FieldResolution
    {
        private MemberInfo member;

        private IdentifierResolution name;

        private Type type;

        /// <summary>
        /// The resolved member reflection info.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the reflection info is set to null.
        /// </exception>
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
        /// The field or property name.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the name is set to null.
        /// </exception>
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
        /// The field or property type.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the type is set to null.
        /// </exception>
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

        /// <summary>
        /// Creates a new field resolution.
        /// </summary>
        /// <param name="member">
        /// The resolved member reflection info.
        /// </param>
        /// <param name="type">
        /// The field or property type.
        /// </param>
        /// <param name="name">
        /// The field or property name.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when reflection info, type, or name is null.
        /// </exception>
        public FieldResolution(MemberInfo member, Type type, IdentifierResolution name)
        {
            Member = member;
            Name = name;
            Type = type;
        }
    }
}
