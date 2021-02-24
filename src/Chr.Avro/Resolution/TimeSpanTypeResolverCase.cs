namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="TimeSpan" />.
    /// </summary>
    public class TimeSpanTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves duration <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="DurationResolution" />
        /// if <paramref name="type" /> is <see cref="TimeSpan" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(TimeSpan))
            {
                return TypeResolverCaseResult.FromTypeResolution(new DurationResolution(type, 1m / TimeSpan.TicksPerSecond));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(TimeSpanTypeResolverCase)} can only be applied to {typeof(TimeSpan)}."));
            }
        }
    }
}
