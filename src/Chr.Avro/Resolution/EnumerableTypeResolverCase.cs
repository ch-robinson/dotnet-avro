namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches <see cref="IEnumerable{T}" />.
    /// </summary>
    public class EnumerableTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="EnumerableTypeResolverCase" /> class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select constructors.
        /// </param>
        public EnumerableTypeResolverCase(BindingFlags memberVisibility)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Gets the binding flags used to select constructors.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Resolves enumerable <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with an <see cref="ArrayResolution" />
        /// if <paramref name="type" /> is <see cref="IEnumerable{T}" />; an unsuccessful
        /// <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type.GetEnumerableType() is Type itemType)
            {
                var constructors = type.IsArray
                    ? null
                    : type.GetConstructors(MemberVisibility)
                        .Select(constructor => new ConstructorResolution(
                            constructor,
                            constructor.GetParameters()
                                .Select(parameter => new ParameterResolution(
                                    parameter,
                                    new IdentifierResolution(parameter.Name)))));

                return TypeResolverCaseResult.FromTypeResolution(new ArrayResolution(type, itemType, constructors));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(EnumerableTypeResolverCase)} can only be applied to {typeof(IEnumerable<>)}."));
            }
        }
    }
}
