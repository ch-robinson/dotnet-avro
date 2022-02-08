namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Represents an Avro schema that defines a union of schemas.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Unions">Avro spec</a> for
    /// details.
    /// </remarks>
    public class UnionSchema : ComplexSchema
    {
        private ConstrainedSet<Schema> schemas = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="UnionSchema" /> class.
        /// </summary>
        /// <param name="schemas">
        /// The union members. If no <see cref="ICollection{Schema}" /> is provided, the union
        /// will be empty after initialization.
        /// </param>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema of the same type already exists as a member of the union.
        /// </exception>
        public UnionSchema(IEnumerable<Schema>? schemas = null)
        {
            Schemas = schemas?.ToArray() ?? Array.Empty<Schema>();
        }

        /// <summary>
        /// Gets or sets the union members.
        /// </summary>
        /// <remarks>
        /// This collection will enforce schema type constraints, namely that union schemas may not
        /// contain other union schemas or multiple schemas of the same type. Duplicate name
        /// constraints, however, are not enforced.
        /// </remarks>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema of the same type already exists as a member of the union.
        /// </exception>
        public ICollection<Schema> Schemas
        {
            get
            {
                return schemas ?? throw new InvalidOperationException();
            }

            set
            {
                schemas = value?.ToConstrainedSet((schema, set) =>
                {
                    if (schema == null)
                    {
                        throw new ArgumentNullException(nameof(value), "A union member cannot be null.");
                    }

                    if (schema is UnionSchema)
                    {
                        throw new InvalidSchemaException("A union may not immediately contain another union.");
                    }

                    if (schema is not NamedSchema && set.Any(existing => existing.GetType() == schema.GetType()))
                    {
                        throw new InvalidSchemaException("A union may not contain more than one schema of the same type.");
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Union schema collection may not be null.");
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{base.ToString()}[{string.Join(", ", schemas)}]";
        }
    }
}
