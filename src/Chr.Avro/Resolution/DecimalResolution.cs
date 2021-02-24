namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about a decimal-like <see cref="Type" />.
    /// </summary>
    public class DecimalResolution : TypeResolution
    {
        private int precision;

        private int scale;

        /// <summary>
        /// Initializes a new instance of the <see cref="DecimalResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="precision">
        /// The total number of digits.
        /// </param>
        /// <param name="scale">
        /// The number of digits to the right of the decimal point.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="precision" /> is less than <c>1</c> or less than
        /// <paramref name="scale" /> or <paramref name="scale" /> is less than <c>0</c> or greater
        /// than <paramref name="precision" />.
        /// </exception>
        public DecimalResolution(Type type, int precision, int scale, bool isNullable = false)
            : base(type, isNullable)
        {
            Precision = precision;
            Scale = scale;
        }

        /// <summary>
        /// Gets or sets the total number of digits.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <see cref="Precision" /> is set to a value less than <c>1</c> or less than
        /// <see cref="Scale" />.
        /// </exception>
        public virtual int Precision
        {
            get
            {
                return precision;
            }

            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be greater than zero.");
                }

                if (value < Scale)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be greater than scale.");
                }

                precision = value;
            }
        }

        /// <summary>
        /// Gets or sets the number of digits to the right of the decimal point.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the <see cref="Scale" /> is set to a value less than <c>0</c> or greater
        /// than <see cref="Precision" />.
        /// </exception>
        public virtual int Scale
        {
            get
            {
                return scale;
            }

            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Scale must be a positive integer.");
                }

                if (value > Precision)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Scale must be less than or equal to precision.");
                }

                scale = value;
            }
        }
    }
}
