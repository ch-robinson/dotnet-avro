namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// An Avro schema representing a record (a data structure with a fixed number of fields).
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#schema_record">Avro spec</a>
    /// for details.
    /// </remarks>
    public class RecordSchema : NamedSchema
    {
        private ConstrainedSet<RecordField> fields = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordSchema" /> class.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <param name="fields">
        /// The record fields.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro specification.
        /// </exception>
        public RecordSchema(string name, IEnumerable<RecordField>? fields = null)
            : base(name)
        {
            Fields = fields?.ToArray() ?? Array.Empty<RecordField>();
        }

        /// <summary>
        /// Gets or sets the human-readable description of the record.
        /// </summary>
        public string? Documentation { get; set; }

        /// <summary>
        /// Gets or sets the record fields.
        /// </summary>
        /// <remarks>
        /// Avro doesn’t allow duplicate field names, but that constraint isn’t enforced here—the
        /// onus is on the user to ensure that the record schema is valid.
        /// </remarks>
        public ICollection<RecordField> Fields
        {
            get
            {
                return fields ?? throw new InvalidOperationException();
            }

            set
            {
                fields = value?.ToConstrainedSet((field, set) =>
                {
                    if (field == null)
                    {
                        throw new ArgumentNullException(nameof(value), "A record field cannot be null.");
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Record field collection cannot be null.");
            }
        }
    }
}
