namespace Chr.Avro.Resolution
{
    using System;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="Nullable{T}" />.
    /// </summary>
    public class NullableTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NullableTypeResolverCase" /> class.
        /// </summary>
        /// <param name="typeResolver">
        /// A resolver instance to use to resolve underlying <see cref="Type" />s.
        /// </param>
        public NullableTypeResolverCase(ITypeResolver typeResolver)
        {
            TypeResolver = typeResolver ?? throw new ArgumentNullException(nameof(typeResolver), "Type resolver cannot be null.");
        }

        /// <summary>
        /// Gets the resolver instance used to resolve underlying <see cref="Type" />s.
        /// </summary>
        public ITypeResolver TypeResolver { get; }

        /// <summary>
        /// Resolves nullable <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> if <paramref name="type" /> is
        /// <see cref="Nullable{T}" />; an unsuccessful <see cref="TypeResolverCaseResult" /> with
        /// an <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            var underlyingType = type.GetUnderlyingType();

            if (type != underlyingType)
            {
                var resolution = TypeResolver.ResolveType(underlyingType);
                resolution.IsNullable = true;
                resolution.Type = type;

                return TypeResolverCaseResult.FromTypeResolution(resolution);
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(NullableTypeResolverCase)} can only be applied to {typeof(Nullable<>)}."));
            }
        }
    }
}
