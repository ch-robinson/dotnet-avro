namespace Chr.Avro.Resolution
{
    using System;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="Enum" />s
    /// and resolves underlying integral <see cref="Type" />s.
    /// </summary>
    public class EnumUnderlyingTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EnumUnderlyingTypeResolverCase" /> class.
        /// </summary>
        /// <param name="typeResolver">
        /// A resolver instance to use to resolve underlying <see cref="Type" />s.
        /// </param>
        public EnumUnderlyingTypeResolverCase(ITypeResolver typeResolver)
        {
            TypeResolver = typeResolver ?? throw new ArgumentNullException(nameof(typeResolver), "Type resolver cannot be null.");
        }

        /// <summary>
        /// Gets the resolver instance used to resolve underlying <see cref="Type" />s.
        /// </summary>
        public ITypeResolver TypeResolver { get; }

        /// <summary>
        /// Resolves underlying <see cref="Enum" /> <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> if <paramref name="type" /> is an
        /// <see cref="Enum" />; an unsuccessful <see cref="TypeResolverCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type.IsEnum)
            {
                var resolution = TypeResolver.ResolveType(type.GetEnumUnderlyingType());
                resolution.Type = type;

                return TypeResolverCaseResult.FromTypeResolution(resolution);
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(EnumUnderlyingTypeResolverCase)} can only be applied to {typeof(Enum)} types."));
            }
        }
    }
}
