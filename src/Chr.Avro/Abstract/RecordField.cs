namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Represents a field in a <see cref="RecordSchema" />.
    /// </summary>
    public sealed class RecordField : Declaration
    {
        private string name = default!;

        private Schema type = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="RecordField" /> class.
        /// </summary>
        /// <param name="name">
        /// The field name.
        /// </param>
        /// <param name="type">
        /// The field type.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the field name does not conform to the Avro naming rules.
        /// </exception>
        public RecordField(string name, Schema type)
        {
            Name = name;
            Type = type;
        }

        /// <summary>
        /// Gets or sets the human-readable description of the field.
        /// </summary>
        public string? Documentation { get; set; }

        /// <summary>
        /// Gets or sets the name of the field.
        /// </summary>
        /// <exception cref="InvalidNameException">
        /// Thrown when the name is set to a value that does not conform to the Avro naming rules.
        /// </exception>
        public string Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }

            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value), "Field name cannot be null.");
                }

                if (!AllowedName.Match(value).Success)
                {
                    throw new InvalidNameException(value);
                }

                name = value;
            }
        }

        /// <summary>
        /// Gets or sets the type of the field.
        /// </summary>
        public Schema Type
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
