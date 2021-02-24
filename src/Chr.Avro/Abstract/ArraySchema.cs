namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Represents an Avro schema that defines an array. Arrays contain a variable number of items
    /// of a single type.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Arrays">Avro spec</a> for
    /// details.
    /// </remarks>
    public class ArraySchema : ComplexSchema
    {
        private Schema item = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="ArraySchema" /> class.
        /// </summary>
        /// <param name="item">
        /// The schema of the items in the array.
        /// </param>
        public ArraySchema(Schema item)
        {
            Item = item;
        }

        /// <summary>
        /// Gets or sets the schema of the items in the array.
        /// </summary>
        public Schema Item
        {
            get
            {
                return item ?? throw new InvalidOperationException();
            }

            set
            {
                item = value ?? throw new ArgumentNullException(nameof(value), "Array item schema cannot be null.");
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{base.ToString()}[{Item}]";
        }
    }
}
