namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="uint" />.
    /// </summary>
    public class UInt32TypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves uint (32-bit unsigned integer) <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="IntegerResolution" />
        /// if <paramref name="type" /> is <see cref="uint" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(uint))
            {
                return TypeResolverCaseResult.FromTypeResolution(new IntegerResolution(type, false, 32));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(UInt32TypeResolverCase)} can only be applied to {typeof(uint)}."));
            }
        }
    }
}
