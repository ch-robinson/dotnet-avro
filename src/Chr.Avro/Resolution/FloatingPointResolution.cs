namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Represents resolved information about an IEEE 754 floating-point number-like <see cref="Type" />.
    /// </summary>
    public class FloatingPointResolution : TypeResolution
    {
        private int size;

        /// <summary>
        /// Initializes a new instance of the <see cref="FloatingPointResolution" /> class.
        /// </summary>
        /// <param name="type">
        /// The resolved <see cref="Type" />.
        /// </param>
        /// <param name="size">
        /// The size of <paramref name="type" /> in bits.
        /// </param>
        /// <param name="isNullable">
        /// Whether <paramref name="type" /> can have a <c>null</c> value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <paramref name="size" /> is less than <c>0</c> or not divisible by
        /// <c>8</c>.
        /// </exception>
        public FloatingPointResolution(Type type, int size, bool isNullable = false)
            : base(type, isNullable)
        {
            Size = size;
        }

        /// <summary>
        /// Gets or sets the size of the floating-point <see cref="Type" /> in bits.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when <see cref="Size" /> is set to a negative value or a value not divisible by
        /// <c>8</c>.
        /// </exception>
        public virtual int Size
        {
            get
            {
                return size;
            }

            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Floating-point size must be nonnegative.");
                }

                if (value % sizeof(byte) != 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), $"Floating-point size must be divisible by {sizeof(byte)}.");
                }

                size = value;
            }
        }
    }
}
