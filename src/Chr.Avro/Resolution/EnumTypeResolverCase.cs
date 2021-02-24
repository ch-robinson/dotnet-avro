namespace Chr.Avro.Resolution
{
    using System;
    using System.Linq;
    using System.Reflection;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="Enum" />s.
    /// </summary>
    public class EnumTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Resolves <see cref="Enum" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with an <see cref="EnumResolution" />
        /// if <paramref name="type" /> is an <see cref="Enum" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type.IsEnum)
            {
                var name = new IdentifierResolution(type.Name);

                var @namespace = string.IsNullOrEmpty(type.Namespace)
                    ? null
                    : new IdentifierResolution(type.Namespace);

                var isFlagEnum = type.GetAttribute<FlagsAttribute>() != null;

                // enum fields will always be public static, so no need to expose binding flags:
                var symbols = type.GetFields(BindingFlags.Public | BindingFlags.Static)
                    .Select(field => new SymbolResolution(
                        field,
                        new IdentifierResolution(field.Name),
                        Enum.Parse(type, field.Name)))
                    .OrderBy(resolution => resolution.Value)
                    .ThenBy(resolution => resolution.Name.Value);

                return TypeResolverCaseResult.FromTypeResolution(new EnumResolution(type, type.GetEnumUnderlyingType(), name, @namespace, isFlagEnum, symbols));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(EnumTypeResolverCase)} can only be applied to {typeof(Enum)} types."));
            }
        }
    }
}
