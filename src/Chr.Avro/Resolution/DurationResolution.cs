namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a duration-like <see cref="Type" />.
    /// </summary>
    public class DurationResolution : TypeResolution
    {
        private decimal precision;

        /// <summary>
        /// Initializes a new instance of the <see cref="DurationResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="precision">
        /// The precision of the duration relative to 1 second. For example, millisecond precision
        /// = <c>0.001</c>.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="precision" /> is less than <c>0</c>.
        /// </exception>
        public DurationResolution(Type type, decimal precision, bool isNullable = false)
            : base(type, isNullable)
        {
            Precision = precision;
        }

        /// <summary>
        /// Gets or sets the precision of the duration relative to 1 second. For example,
        /// millisecond precision = <c>0.001</c>.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <see cref="Precision" /> is set to a value less than <c>0</c>.
        /// </exception>
        public virtual decimal Precision
        {
            get
            {
                return precision;
            }

            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be a positive factor.");
                }

                precision = value;
            }
        }
    }
}
