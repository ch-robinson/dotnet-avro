namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="sbyte" />.
    /// </summary>
    public class SByteTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves sbyte (8-bit signed integer) <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="IntegerResolution" />
        /// if <paramref name="type" /> is <see cref="sbyte" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type == typeof(sbyte))
            {
                return TypeResolverCaseResult.FromTypeResolution(new IntegerResolution(type, true, 8));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(SByteTypeResolverCase)} can only be applied to {typeof(sbyte)}."));
            }
        }
    }
}
