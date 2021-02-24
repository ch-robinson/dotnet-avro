namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="ulong" />.
    /// </summary>
    public class UInt64TypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves ulong (64-bit unsigned integer) <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="IntegerResolution" />
        /// if <paramref name="type" /> is <see cref="ulong" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(ulong))
            {
                return TypeResolverCaseResult.FromTypeResolution(new IntegerResolution(type, false, 64));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(UInt64TypeResolverCase)} can only be applied to {typeof(ulong)}."));
            }
        }
    }
}
