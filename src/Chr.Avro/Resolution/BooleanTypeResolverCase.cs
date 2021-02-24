namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="bool" />.
    /// </summary>
    public class BooleanTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves Boolean <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="BooleanResolution" />
        /// if <paramref name="type" /> is <see cref="bool" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(bool))
            {
                return TypeResolverCaseResult.FromTypeResolution(new BooleanResolution(type));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BooleanTypeResolverCase)} can only be applied to {typeof(bool)}."));
            }
        }
    }
}
