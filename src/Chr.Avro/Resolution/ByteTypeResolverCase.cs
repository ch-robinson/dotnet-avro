namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="byte" />.
    /// </summary>
    public class ByteTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves byte (8-bit unsigned integer) <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="IntegerResolution" />
        /// if <paramref name="type" /> is <see cref="byte" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(byte))
            {
                return TypeResolverCaseResult.FromTypeResolution(new IntegerResolution(type, false, 8));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(ByteTypeResolverCase)} can only be applied to {typeof(byte)}."));
            }
        }
    }
}
