namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a base deserializer builder case implementation.
    /// </summary>
    public abstract class DeserializerBuilderCase
    {
        /// <summary>
        /// Builds an <see cref="Expression" /> representing a conversion from an intermediate
        /// <see cref="Type" /> to a target <see cref="Type" />.
        /// </summary>
        /// <remarks>
        /// See the remarks for <see cref="Expression.ConvertChecked(Expression, Type)" />.
        /// </remarks>
        /// <param name="value">
        /// An <see cref="Expression" /> representing an intermediately typed value (a value used
        /// by a generated deserializer internally prior to converting and returning).
        /// </param>
        /// <param name="target">
        /// A <see cref="Type" /> to convert <paramref name="value" /> to (the return type of the
        /// generated deserializer).
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the conversion of <paramref name="value" />
        /// to <paramref name="target" />. If <paramref name="value" /> already has type
        /// <paramref name="target" />, <paramref name="value" /> is returned as-is.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when no conversion to <paramref name="target" /> can be built.
        /// </exception>
        protected virtual Expression BuildConversion(Expression value, Type target)
        {
            if (value.Type == target)
            {
                return value;
            }

            return Expression.ConvertChecked(value, target);
        }
    }
}
