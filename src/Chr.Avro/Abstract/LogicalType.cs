using System;

namespace Chr.Avro.Abstract
{
    /// <summary>
    /// A custom Avro type that can decorate one or more built-in types.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Logical+Types">Avro spec</a>
    /// for details.
    /// </remarks>
    public abstract class LogicalType { }

    /// <summary>
    /// A logical type representing a calendar date (with no reference to a
    /// particular time zone or time of day) as days from the Unix epoch.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Date">Avro spec</a> for
    /// details.
    /// </remarks>
    public sealed class DateLogicalType : LogicalType { }

    /// <summary>
    /// A logical type representing an arbitrary-precision signed decimal number.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Decimal">Avro spec</a> for
    /// details.
    /// </remarks>
    public sealed class DecimalLogicalType : LogicalType
    {
        private int precision;

        private int scale;

        /// <summary>
        /// The number of digits.
        /// </summary>
        /// <throws cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is set to a value less than one or less than the scale.
        /// </throws>
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
        /// The number of digits to the right of the decimal point.
        /// </summary>
        /// <throws cref="ArgumentOutOfRangeException">
        /// Thrown when the scale is set to a value less than zero or greater than the precision.
        /// </throws>
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

        /// <summary>
        /// Creates a new decimal logical type.
        /// </summary>
        /// <param name="precision">
        /// The number of digits.
        /// </param>
        /// <param name="scale">
        /// The number of digits to the right of the decimal point.
        /// </param>
        /// <throws cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is less than one or less than the scale or the scale is less
        /// than zero or greater than the precision.
        /// </throws>
        public DecimalLogicalType(int precision, int scale)
        {
            Precision = precision;
            Scale = scale;
        }
    }

    /// <summary>
    /// A logical type representing an amount of time defined by a number of months, days, and
    /// milliseconds.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Duration">Avro spec</a> for
    /// details.
    /// </remarks>
    public sealed class DurationLogicalType : LogicalType
    {
        /// <summary>
        /// The size of a duration (three 32-bit unsigned integers).
        /// </summary>
        public const int DurationSize = 12;
    }
    
    /// <summary>
    /// A logical type that represents a time of day.
    /// </summary>
    public abstract class TimeLogicalType : LogicalType { }

    /// <summary>
    /// A logical type representing a time of day (with no reference to a particular time zone) as
    /// microseconds after midnight.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Time+(microsecond+precision)">Avro spec</a>
    /// for details.
    /// </remarks>
    public sealed class MicrosecondTimeLogicalType : TimeLogicalType { }

    /// <summary>
    /// A logical type representing a time of day (with no reference to a particular time zone) as
    /// milliseconds after midnight.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Time+(millisecond+precision)">Avro spec</a>
    /// for details.
    /// </remarks>
    public sealed class MillisecondTimeLogicalType : TimeLogicalType { }

    /// <summary>
    /// A logical type that represents an instant in time.
    /// </summary>
    public abstract class TimestampLogicalType : LogicalType { }

    /// <summary>
    /// A logical type representing an instant in time as microseconds from the Unix epoch.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Timestamp+(microsecond+precision)">Avro spec</a>
    /// for details.
    /// </remarks>
    public sealed class MicrosecondTimestampLogicalType : TimestampLogicalType { }

    /// <summary>
    /// A logical type representing an instant in time as milliseconds from the Unix epoch.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Timestamp+(millisecond+precision)">Avro spec</a>
    /// for details.
    /// </remarks>
    public sealed class MillisecondTimestampLogicalType : TimestampLogicalType { }
}
