namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Represents an Avro logical type that defines an arbitrary-precision signed decimal number.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Decimal">Avro spec</a> for
    /// details.
    /// </remarks>
    public class DecimalLogicalType : LogicalType
    {
        private int precision;

        private int scale;

        /// <summary>
        /// Initializes a new instance of the <see cref="DecimalLogicalType" /> class.
        /// </summary>
        /// <param name="precision">
        /// The total number of digits.
        /// </param>
        /// <param name="scale">
        /// The number of digits to the right of the decimal point.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is less than one or less than the scale or the scale is less
        /// than zero or greater than the precision.
        /// </exception>
        public DecimalLogicalType(int precision, int scale)
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
        public int Precision
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
        public int Scale
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

        /// <inheritdoc />
        public override string ToString()
        {
            return $"{base.ToString()}({Precision}, {Scale})";
        }
    }
}
