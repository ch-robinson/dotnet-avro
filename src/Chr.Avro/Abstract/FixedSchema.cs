namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Represents an Avro schema that defines a fixed number of bytes.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Fixed">Avro spec</a> for
    /// details.
    /// </remarks>
    public class FixedSchema : NamedSchema
    {
        private int size;

        /// <summary>
        /// Initializes a new instance of the <see cref="FixedSchema" /> class.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <param name="size">
        /// The number of bytes per value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the size is negative.
        /// </exception>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro specification.
        /// </exception>
        public FixedSchema(string name, int size)
            : base(name)
        {
            Size = size;
        }

        /// <summary>
        /// Gets or sets the number of bytes.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when size is set to a negative value.
        /// </exception>
        public int Size
        {
            get
            {
                return size;
            }

            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("A fixed schema cannot have a negative size.");
                }

                size = value;
            }
        }
    }
}
