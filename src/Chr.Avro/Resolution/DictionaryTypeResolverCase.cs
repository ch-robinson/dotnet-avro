namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Implements a <see cref="TypeResolver" /> case that matches
    /// <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
    /// </summary>
    public class DictionaryTypeResolverCase : TypeResolverCase, ITypeResolverCase<TypeResolverCaseResult>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DictionaryTypeResolverCase" /> class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select constructors.
        /// </param>
        public DictionaryTypeResolverCase(BindingFlags memberVisibility)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Gets the binding flags used to select constructors.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Resolves dictionary <see cref="Type" /> information.
        /// </summary>
        /// <returns>
        /// A successful <see cref="TypeResolverCaseResult" /> with a <see cref="MapResolution" />
        /// if <paramref name="type" /> is <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />;
        /// an unsuccessful <see cref="TypeResolverCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual TypeResolverCaseResult ResolveType(Type type)
        {
            if (type.GetEnumerableType() is Type pairType && pairType.IsGenericType && pairType.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
            {
                var arguments = pairType.GetGenericArguments();
                var constructors = type.GetConstructors(MemberVisibility)
                    .Select(constructor => new ConstructorResolution(
                        constructor,
                        constructor.GetParameters()
                            .Select(parameter => new ParameterResolution(
                                parameter,
                                new IdentifierResolution(parameter.Name)))));

                return TypeResolverCaseResult.FromTypeResolution(new MapResolution(type, arguments[0], arguments[1], constructors));
            }
            else
            {
                return TypeResolverCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(DictionaryTypeResolverCase)} can only be applied to {typeof(IEnumerable<>).MakeGenericType(typeof(KeyValuePair<,>))}."));
            }
        }
    }
}
