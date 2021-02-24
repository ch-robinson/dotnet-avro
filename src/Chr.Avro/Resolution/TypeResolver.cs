namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    /// <summary>
    /// Resolves information for .NET <see cref="Type" />s.
    /// </summary>
    public class TypeResolver : ITypeResolver
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TypeResolver" /> class configured with the
        /// default list of cases.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="resolveUnderlyingEnumTypes">
        /// Whether to resolve enum types as their underlying integral <see cref="Type" />s.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public TypeResolver(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance,
            bool resolveReferenceTypesAsNullable = false,
            bool resolveUnderlyingEnumTypes = false)
        : this(CreateDefaultCaseBuilders(memberVisibility, resolveUnderlyingEnumTypes), resolveReferenceTypesAsNullable)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TypeResolver" /> class.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        public TypeResolver(
            IEnumerable<Func<ITypeResolver, ITypeResolverCase<TypeResolverCaseResult>>> caseBuilders,
            bool resolveReferenceTypesAsNullable = false)
        {
            var cases = new List<ITypeResolverCase<TypeResolverCaseResult>>();

            Cases = cases;
            ResolveReferenceTypesAsNullable = resolveReferenceTypesAsNullable;

            // initialize cases last so that the type resolver is fully ready:
            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Gets the list of cases that the type resolver will attempt to apply. If the first case
        /// does not match, the type resolver will try the next case, and so on until all cases
        /// have been tried.
        /// </summary>
        public virtual IEnumerable<ITypeResolverCase<TypeResolverCaseResult>> Cases { get; }

        /// <summary>
        /// Gets a value indicating whether reference <see cref="Type" />s should be resolved as
        /// nullable.
        /// </summary>
        public virtual bool ResolveReferenceTypesAsNullable { get; }

        /// <summary>
        /// Creates a default list of case builders that rely on <see cref="Type" /> information as
        /// well as <see cref="System.Runtime.Serialization" /> attributes.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="resolveUnderlyingEnumTypes">
        /// Whether to resolve enum types as their underlying integral <see cref="Type" />s.
        /// </param>
        /// <returns>
        /// A list of case builders capable of handling most .NET <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<ITypeResolver, ITypeResolverCase<TypeResolverCaseResult>>> CreateDefaultCaseBuilders(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance,
            bool resolveUnderlyingEnumTypes = false)
        {
            return new Func<ITypeResolver, ITypeResolverCase<TypeResolverCaseResult>>[]
            {
                // nullables:
                resolver => new NullableTypeResolverCase(resolver),

                // primitives:
                resolver => new BooleanTypeResolverCase(),
                resolver => new ByteTypeResolverCase(),
                resolver => new ByteArrayTypeResolverCase(),
                resolver => new DecimalTypeResolverCase(),
                resolver => new DoubleTypeResolverCase(),
                resolver => new SingleTypeResolverCase(),
                resolver => new Int16TypeResolverCase(),
                resolver => new Int32TypeResolverCase(),
                resolver => new Int64TypeResolverCase(),
                resolver => new SByteTypeResolverCase(),
                resolver => new StringTypeResolverCase(),
                resolver => new UInt16TypeResolverCase(),
                resolver => new UInt32TypeResolverCase(),
                resolver => new UInt64TypeResolverCase(),

                // enums:
                resolver => resolveUnderlyingEnumTypes
                    ? new EnumUnderlyingTypeResolverCase(resolver)
                    : new DataContractEnumTypeResolverCase(),

                // dictionaries:
                resolver => new DictionaryTypeResolverCase(memberVisibility),

                // enumerables:
                resolver => new EnumerableTypeResolverCase(memberVisibility),

                // built-ins:
                resolver => new DateTimeTypeResolverCase(),
                resolver => new GuidTypeResolverCase(),
                resolver => new TimeSpanTypeResolverCase(),
                resolver => new UriTypeResolverCase(),

                // classes and structs:
                resolver => new DataContractObjectTypeResolverCase(memberVisibility),
            };
        }

        /// <summary>
        /// Creates a list of case builders that rely only on <see cref="Type" /> information.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="resolveUnderlyingEnumTypes">
        /// Whether to resolve enum types as their underlying integral <see cref="Type" />s.
        /// </param>
        /// <returns>
        /// A list of case builders capable of handling most .NET <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<ITypeResolver, ITypeResolverCase<TypeResolverCaseResult>>> CreateReflectionCaseBuilders(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance,
            bool resolveUnderlyingEnumTypes = false)
        {
            return new Func<ITypeResolver, ITypeResolverCase<TypeResolverCaseResult>>[]
            {
                // nullables:
                resolver => new NullableTypeResolverCase(resolver),

                // primitives:
                resolver => new BooleanTypeResolverCase(),
                resolver => new ByteTypeResolverCase(),
                resolver => new ByteArrayTypeResolverCase(),
                resolver => new DecimalTypeResolverCase(),
                resolver => new DoubleTypeResolverCase(),
                resolver => new SingleTypeResolverCase(),
                resolver => new Int16TypeResolverCase(),
                resolver => new Int32TypeResolverCase(),
                resolver => new Int64TypeResolverCase(),
                resolver => new SByteTypeResolverCase(),
                resolver => new StringTypeResolverCase(),
                resolver => new UInt16TypeResolverCase(),
                resolver => new UInt32TypeResolverCase(),
                resolver => new UInt64TypeResolverCase(),

                // enums:
                resolver => resolveUnderlyingEnumTypes
                    ? new EnumUnderlyingTypeResolverCase(resolver)
                    : new EnumTypeResolverCase(),

                // dictionaries:
                resolver => new DictionaryTypeResolverCase(memberVisibility),

                // enumerables:
                resolver => new EnumerableTypeResolverCase(memberVisibility),

                // built-ins:
                resolver => new DateTimeTypeResolverCase(),
                resolver => new GuidTypeResolverCase(),
                resolver => new TimeSpanTypeResolverCase(),
                resolver => new UriTypeResolverCase(),

                // classes and structs:
                resolver => new ObjectTypeResolverCase(memberVisibility),
            };
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case matches <typeparamref name="T" /> or when a matching case fails.
        /// </exception>
        /// <inheritdoc />
        public virtual TypeResolution ResolveType<T>()
        {
            return ResolveType(typeof(T));
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case matches <paramref name="type" /> or when a matching case fails.
        /// </exception>
        /// <inheritdoc />
        public virtual TypeResolution ResolveType(Type type)
        {
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.ResolveType(type);

                if (result.TypeResolution != null)
                {
                    if (ResolveReferenceTypesAsNullable && !result.TypeResolution.Type.IsValueType)
                    {
                        result.TypeResolution.IsNullable = true;
                    }

                    return result.TypeResolution;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnsupportedTypeException(type, $"No type resolver case could be applied to {type}.", new AggregateException(exceptions));
        }
    }
}
