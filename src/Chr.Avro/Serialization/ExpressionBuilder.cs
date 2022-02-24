namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Microsoft.CSharp.RuntimeBinder;

    /// <summary>
    /// Provides a shared base for classes that build <see cref="Expression" />s.
    /// </summary>
    public abstract class ExpressionBuilder
    {
        /// <summary>
        /// Builds an <see cref="Expression" /> representing a conversion to a target
        /// <see cref="Type" />.
        /// </summary>
        /// <remarks>
        /// Inheriting classes should override <see cref="BuildDynamicConversion" /> and
        /// <see cref="BuildStaticConversion" /> to provide additional conversions.
        /// </remarks>
        /// <param name="value">
        /// An <see cref="Expression" /> representing an intermediately typed value (a value used
        /// by a generated serializer or deserializer internally prior to converting and returning).
        /// </param>
        /// <param name="target">
        /// A <see cref="Type" /> to convert <paramref name="value" /> to (the return type of the
        /// generated serializer or deserializer).
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the conversion of <paramref name="value" />
        /// to <paramref name="target" />. If <paramref name="value" /> already has type
        /// <paramref name="target" />, <paramref name="value" /> is returned as-is.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        /// Thrown when no conversion to <paramref name="target" /> can be built.
        /// </exception>
        protected Expression BuildConversion(Expression value, Type target)
        {
            // bail early if the type already matches:
            if (value.Type == target)
            {
                return value;
            }

            // if the type is unknown, generate a dynamic conversion:
            else if (value.Type == typeof(object))
            {
                return BuildDynamicConversion(value, target);
            }

            // in all other cases, generate a static conversion:
            else
            {
                return BuildStaticConversion(value, target);
            }
        }

        /// <summary>
        /// Builds an <see cref="Expression" /> representing a dynamic conversion to a target
        /// <see cref="Type" />.
        /// </summary>
        /// <param name="value">
        /// An <see cref="Expression" /> representing an intermediately typed value (a value used
        /// by a generated serializer or deserializer internally prior to converting and returning).
        /// </param>
        /// <param name="target">
        /// A <see cref="Type" /> to convert <paramref name="value" /> to (the return type of the
        /// generated serializer or deserializer).
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the static conversion of <paramref name="value" />
        /// to <paramref name="target" />.
        /// </returns>
        protected virtual Expression BuildDynamicConversion(Expression value, Type target)
        {
            return Expression.Dynamic(
                Binder.Convert(
                    CSharpBinderFlags.CheckedContext | CSharpBinderFlags.ConvertExplicit,
                    target,
                    value.Type),
                target,
                value);
        }

        /// <summary>
        /// Builds an <see cref="Expression" /> representing a static conversion to a target
        /// <see cref="Type" />.
        /// </summary>
        /// <param name="value">
        /// An <see cref="Expression" /> representing an intermediately typed value (a value used
        /// by a generated serializer or deserializer internally prior to converting and returning).
        /// </param>
        /// <param name="target">
        /// A <see cref="Type" /> to convert <paramref name="value" /> to (the return type of the
        /// generated serializer or deserializer).
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing the static conversion of <paramref name="value" />
        /// to <paramref name="target" />.
        /// </returns>
        protected virtual Expression BuildStaticConversion(Expression value, Type target)
        {
            return Expression.ConvertChecked(value, target);
        }
    }
}
