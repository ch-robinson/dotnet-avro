namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="DateTime" /> and
    /// <see cref="DateTimeOffset" />.
    /// </summary>
    public class DateTimeTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves date/time <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="TimestampResolution" />
        /// if <paramref name="type" /> is <see cref="DateTime" /> or <see cref="DateTimeOffset" />;
        /// an unsuccessful <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
            {
                return TypeResolverCaseResult.FromTypeResolution(new TimestampResolution(type, 1m / TimeSpan.TicksPerSecond));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BooleanTypeResolverCase)} can only be applied to {typeof(DateTime)} or {typeof(DateTimeOffset)}."));
            }
        }
    }
}
