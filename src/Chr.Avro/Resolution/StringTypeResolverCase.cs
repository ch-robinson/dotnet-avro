namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="string" />.
    /// </summary>
    public class StringTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves string <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="StringResolution" />
        /// if <paramref name="type" /> is <see cref="string" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(string))
            {
                return TypeResolverCaseResult.FromTypeResolution(new StringResolution(type));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(StringTypeResolverCase)} can only be applied to {typeof(string)}."));
            }
        }
    }
}
