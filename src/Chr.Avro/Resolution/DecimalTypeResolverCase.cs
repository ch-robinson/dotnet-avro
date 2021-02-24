namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="decimal" />.
    /// </summary>
    public class DecimalTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves decimal <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="DecimalResolution" />
        /// if <paramref name="type" /> is <see cref="decimal" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(decimal))
            {
                // hard-code precision/scale to .NET limits:
                return TypeResolverCaseResult.FromTypeResolution(new DecimalResolution(type, 29, 14));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(DecimalTypeResolverCase)} can only be applied to {typeof(decimal)}."));
            }
        }
    }
}
