namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="T:System.Byte[]" />.
    /// </summary>
    public class ByteArrayTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves byte array <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="ByteArrayResolution" />
        /// if <paramref name="type" /> is <see cref="T:System.Byte[]" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(byte[]))
            {
                return TypeResolverCaseResult.FromTypeResolution(new ByteArrayResolution(type));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(ByteArrayTypeResolverCase)} can only be applied to {typeof(byte[])}."));
            }
        }
    }
}
