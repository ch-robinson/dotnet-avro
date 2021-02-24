namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Represents an Avro schema that defines a map of string keys to values.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Maps">Avro spec</a> for
    /// details.
    /// </remarks>
    public class MapSchema : ComplexSchema
    {
        private Schema value = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="MapSchema" /> class.
        /// </summary>
        /// <param name="value">
        /// The schema of the values in the map.
        /// </param>
        public MapSchema(Schema value)
        {
            Value = value;
        }

        /// <summary>
        /// Gets or sets the schema of the values in the map.
        /// </summary>
        public Schema Value
        {
            get
            {
                return value ?? throw new InvalidOperationException();
            }

            set
            {
                this.value = value ?? throw new ArgumentNullException(nameof(value), "Map value schema cannot be null.");
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{base.ToString()}[{Value}]";
        }
    }
}
