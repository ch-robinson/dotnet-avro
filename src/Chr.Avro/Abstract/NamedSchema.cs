namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Represents an Avro schema identified by a name and (optionally) aliases. The name and
    /// aliases may each be qualified by a namespace.
    /// </summary>
    public abstract class NamedSchema : ComplexSchema
    {
        private ConstrainedSet<string> aliases = default!;

        private string name = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="NamedSchema" /> class.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro naming rules.
        /// </exception>
        public NamedSchema(string name)
        {
            Aliases = Array.Empty<string>();
            FullName = name;
        }

        /// <summary>
        /// Gets or sets the alternate names by which the schema can be identified.
        /// </summary>
        /// <remarks>
        /// Aliases are fully-qualified; they do not inherit the schema’s namespace. Duplicate
        /// aliases will be filtered from the collection.
        /// </remarks>
        /// <exception cref="InvalidNameException">
        /// Thrown when an alias does not conform to the Avro specification.
        /// </exception>
        public ICollection<string> Aliases
        {
            get
            {
                return aliases ?? throw new InvalidOperationException();
            }

            set
            {
                aliases = value?.ToConstrainedSet((alias, set) =>
                {
                    if (alias == null)
                    {
                        throw new ArgumentNullException(nameof(value), "A schema alias cannot be null.");
                    }

                    if (!alias.Split('.').All(component => AllowedName.Match(component).Success))
                    {
                        throw new InvalidNameException(alias);
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Schema alias collection cannot be null.");
            }
        }

        /// <summary>
        /// Gets or sets the qualified schema name.
        /// </summary>
        /// <exception cref="InvalidNameException">
        /// Thrown when the name is set to a value that does not conform to the Avro naming rules.
        /// </exception>
        public string FullName
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }

            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value), "Schema name cannot be null.");
                }

                if (!value.Split('.').All(component => AllowedName.Match(component).Success))
                {
                    throw new InvalidNameException(value);
                }

                name = value;
            }
        }

        /// <summary>
        /// Gets or sets the unqualified schema name.
        /// </summary>
        /// <remarks>
        /// Setting this property to a qualified name will update the full name, overwriting the
        /// existing namespace. Setting this property to an unqualified name will retain the
        /// existing namespace.
        /// </remarks>
        /// <exception cref="InvalidNameException">
        /// Thrown when the name is set to a value that does not conform to the Avro naming rules.
        /// </exception>
        public string Name
        {
            get
            {
                return FullName.Substring(FullName.LastIndexOf('.') + 1);
            }

            set
            {
                FullName = Namespace != null && value?.IndexOf('.') < 0
                    ? $"{Namespace}.{value}"
                    : value!;
            }
        }

        /// <summary>
        /// Gets or sets the schema namespace.
        /// </summary>
        /// <remarks>
        /// This property will return <c>null</c> if no namespace is set. Setting this property to
        /// <c>null</c> or the empty string will clear the namespace.
        /// </remarks>
        /// <exception cref="InvalidNameException">
        /// Thrown when the namespace is set to a value that does not conform to the Avro naming
        /// rules.
        /// </exception>
        public string? Namespace
        {
            get
            {
                var index = FullName.LastIndexOf('.');

                return index < 0
                    ? null
                    : FullName.Substring(0, index);
            }

            set
            {
                value = string.IsNullOrEmpty(value)
                    ? string.Empty
                    : $"{value}.";

                FullName = $"{value}{Name}";
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{base.ToString()} {FullName}";
        }
    }
}
