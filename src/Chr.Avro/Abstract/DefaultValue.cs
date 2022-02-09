namespace Chr.Avro.Abstract
{
    using System;
    using System.Linq;

    /// <summary>
    /// Wraps the default value of a <see cref="RecordField" />.
    /// </summary>
    public abstract class DefaultValue
    {
        private Schema schema = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultValue" /> class.
        /// </summary>
        /// <param name="schema">
        /// A <see cref="Schema" /> that can be used to read the value.
        /// </param>
        public DefaultValue(Schema schema)
        {
            Schema = schema;
        }

        /// <summary>
        /// Gets or sets the schema that can be used to read the value.
        /// </summary>
        public Schema Schema
        {
            get
            {
                return schema ?? throw new InvalidOperationException();
            }

            set
            {
                schema = value switch
                {
                    null => throw new ArgumentNullException(nameof(value), "Schema cannot be null."),
                    UnionSchema unionSchema => unionSchema.Schemas.First(),
                    Schema schema => schema,
                };
            }
        }

        /// <summary>
        /// Gets the value as a .NET object.
        /// </summary>
        /// <typeparam name="T">
        /// The <see cref="Type" /> to map the value to.
        /// </typeparam>
        /// <returns>
        /// The value as <typeparamref name="T" />.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the value cannot be mapped to <typeparamref name="T" />.
        /// </exception>
        public abstract T? ToObject<T>();
    }
}
