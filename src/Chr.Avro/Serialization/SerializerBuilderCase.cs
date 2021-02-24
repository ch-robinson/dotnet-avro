namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Provides a base serializer builder case implementation.
    /// </summary>
    public abstract class SerializerBuilderCase
    {
        /// <summary>
        /// Builds an <see cref="Expression" /> representing a conversion from a source
        /// <see cref="Type" /> to an intermediate <see cref="Type" />.
        /// </summary>
        /// <remarks>
        /// See the remarks for <see cref="Expression.ConvertChecked(Expression, Type)" />.
        /// </remarks>
        /// <param name="value">
        /// An <see cref="Expression" /> representing a value to be serialized.
        /// </param>
        /// <param name="intermediate">
        /// A <see cref="Type" /> to convert <paramref name="value" /> to (used by the generated
        /// serializer internally).
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the conversion of <paramref name="value" />
        /// to <paramref name="intermediate" />. If <paramref name="value" /> already has type
        /// <paramref name="intermediate" />, <paramref name="value" /> is returned as-is.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when no conversion to <paramref name="intermediate" /> can be built.
        /// </exception>
        protected virtual Expression BuildConversion(Expression value, Type intermediate)
        {
            if (value.Type == intermediate)
            {
                return value;
            }

            return Expression.ConvertChecked(value, intermediate);
        }
    }
}
