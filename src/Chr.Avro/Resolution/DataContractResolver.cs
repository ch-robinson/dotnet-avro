using System;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// A type resolver that extends <see cref="ReflectionResolver" /> with support for
    /// <see cref="System.Runtime.Serialization" /> attributes.
    /// </summary>
    public class DataContractResolver : ReflectionResolver
    {
        /// <summary>
        /// Creates a new data contract resolver.
        /// </summary>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="resolveUnderlyingEnumTypes">
        /// Whether to resolve enum types as their underlying integral types.
        /// </param>
        public DataContractResolver(bool resolveReferenceTypesAsNullable = false, bool resolveUnderlyingEnumTypes = false)
            : base(resolveReferenceTypesAsNullable, resolveUnderlyingEnumTypes)
        {
            Cases = new ITypeResolverCase[]
            {
                // nullables:
                new NullableResolverCase(this),

                // primitives:
                new BooleanResolverCase(),
                new ByteResolverCase(),
                new ByteArrayResolverCase(),
                new DecimalResolverCase(),
                new DoubleResolverCase(),
                new SingleResolverCase(),
                new Int16ResolverCase(),
                new Int32ResolverCase(),
                new Int64ResolverCase(),
                new SByteResolverCase(),
                new StringResolverCase(),
                new UInt16ResolverCase(),
                new UInt32ResolverCase(),
                new UInt64ResolverCase(),

                // enums:
                resolveUnderlyingEnumTypes
                    ? new EnumUnderlyingTypeResolverCase(this)
                    : new DataContractEnumResolverCase() as ITypeResolverCase,

                // dictionaries:
                new DictionaryResolverCase(),

                // enumerables:
                new EnumerableResolverCase(),

                // built-ins:
                new DateTimeResolverCase(),
                new GuidResolverCase(),
                new TimeSpanResolverCase(),
                new UriResolverCase(),

                // classes and structs:
                new DataContractObjectResolverCase(),

                // interfaces and abstract classes
                new InterfaceResolverCase()
            };
        }
    }

    /// <summary>
    /// An <see cref="ITypeResolverCase" /> that uses <see cref="System.Runtime.Serialization" />
    /// attributes to obtain additional type information.
    /// </summary>
    public abstract class DataContractResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Creates a name resolution for a <see cref="DataMemberAttribute" />-annotated type.
        /// </summary>
        protected virtual IdentifierResolution CreateNameResolution(MemberInfo member, DataMemberAttribute attribute)
        {
            return string.IsNullOrEmpty(attribute?.Name)
                ? new IdentifierResolution(member.Name)
                : new IdentifierResolution(attribute.Name, true);
        }

        /// <summary>
        /// Creates a name resolution for a <see cref="EnumMemberAttribute" />-annotated type.
        /// </summary>
        protected virtual IdentifierResolution CreateNameResolution(MemberInfo member, EnumMemberAttribute attribute)
        {
            return string.IsNullOrEmpty(attribute?.Value)
                ? new IdentifierResolution(member.Name)
                : new IdentifierResolution(attribute.Value, true);
        }

        /// <summary>
        /// Creates a name resolution for a <see cref="DataContractAttribute" />-annotated type.
        /// </summary>
        protected virtual IdentifierResolution CreateNameResolution(Type type, DataContractAttribute attribute)
        {
            return string.IsNullOrEmpty(attribute?.Name)
                ? new IdentifierResolution(type.Name)
                : new IdentifierResolution(attribute.Name, true);
        }
        
        /// <summary>
        /// Creates a namespace resolution for a <see cref="DataContractAttribute" />-annotated
        /// type.
        /// </summary>
        protected virtual IdentifierResolution CreateNamespaceResolution(Type type, DataContractAttribute attribute)
        {
            return string.IsNullOrEmpty(attribute?.Namespace)
                ? string.IsNullOrEmpty(type.Namespace)
                    ? null
                    : new IdentifierResolution(type.Namespace)
                : new IdentifierResolution(attribute.Namespace, true);
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Enum" /> types, taking
    /// <see cref="System.Runtime.Serialization" /> attributes into account.
    /// </summary>
    public class DataContractEnumResolverCase : DataContractResolverCase
    {
        /// <summary>
        /// Determines whether the case can be applied to a type.
        /// </summary>
        /// <returns>
        /// Whether the type is an enum type.
        /// </returns>
        public override bool IsMatch(Type type)
        {
            return type.IsEnum;
        }

        /// <summary>
        /// Resolves enum type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// An <see cref="EnumResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not an enum type.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!IsMatch(type))
            {
                throw new ArgumentException("The object resolver can only be applied to non-array, non-primitive types.", nameof(type));
            }

            var contract = GetAttribute<DataContractAttribute>(type);

            var name = CreateNameResolution(type, contract);
            var @namespace = CreateNamespaceResolution(type, contract);

            var isFlagEnum = GetAttribute<FlagsAttribute>(type) != null;

            var symbols = (contract == null
                ? type.GetFields(BindingFlags.Public | BindingFlags.Static)
                    .Select(f => (
                        MemberInfo: f as MemberInfo,
                        Attribute: GetAttribute<NonSerializedAttribute>(f)
                    ))
                    .Where(f => f.Attribute == null)
                    .Select(f => (
                        f.MemberInfo,
                        Name: new IdentifierResolution(f.MemberInfo.Name),
                        Value: Enum.Parse(type, f.MemberInfo.Name)
                    ))
                : type.GetFields(BindingFlags.Public | BindingFlags.Static)
                    .Select(f => (
                        MemberInfo: f as MemberInfo,
                        Attribute: GetAttribute<EnumMemberAttribute>(f)
                    ))
                    .Where(f => f.Attribute != null)
                    .Select(f => (
                        f.MemberInfo,
                        Name: CreateNameResolution(f.MemberInfo, f.Attribute),
                        Value: Enum.Parse(type, f.MemberInfo.Name)
                    )))
                .OrderBy(f => f.Value)
                .ThenBy(f => f.Name.Value)
                .Select(f => new SymbolResolution(f.MemberInfo, f.Name, f.Value))
                .ToList();

            return new EnumResolution(type, name, @namespace, isFlagEnum, symbols);
        }
    }

    /// <summary>
    /// A general type resolver case that inspects fields and properties, taking
    /// <see cref="System.Runtime.Serialization" /> attributes into account.
    /// </summary>
    public class DataContractObjectResolverCase : DataContractResolverCase
    {
        /// <summary>
        /// The binding flags that will be used to select fields and properties.
        /// </summary>
        protected readonly BindingFlags MemberVisibility;

        /// <summary>
        /// Creates a new object resolver case.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select fields and properties. If none are provided,
        /// public instance members will be selected by default.
        /// </param>
        public DataContractObjectResolverCase(BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type.
        /// </summary>
        /// <returns>
        /// Whether the type is a class, interface, or struct.
        /// </returns>
        public override bool IsMatch(Type type)
        {
            return !type.IsArray && !type.IsPrimitive && !type.IsInterface;
        }

        /// <summary>
        /// Resolves class, interface, or struct type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A <see cref="RecordResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the type is an array type or a primitive type.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!IsMatch(type))
            {
                throw new ArgumentException("The object resolver can only be applied to non-array, non-primitive types.", nameof(type));
            }

            var contract = GetAttribute<DataContractAttribute>(type);

            var name = CreateNameResolution(type, contract);
            var @namespace = CreateNamespaceResolution(type, contract);

            var fields = (contract == null
                ? GetMembers(type, MemberVisibility)
                    .Select(m => (
                        m.MemberInfo,
                        m.Type,
                        Attribute: GetAttribute<NonSerializedAttribute>(m.MemberInfo)
                    ))
                    .Where(m => m.Attribute == null)
                    .Select(m => (
                        m.MemberInfo,
                        m.Type,
                        Name: new IdentifierResolution(m.MemberInfo.Name),
                        Order: 0
                    ))
                : GetMembers(type, MemberVisibility)
                    .Select(m => (
                        m.MemberInfo,
                        m.Type,
                        Attribute: GetAttribute<DataMemberAttribute>(m.MemberInfo)
                    ))
                    .Where(m => m.Attribute != null)
                    .Select(m => (
                        m.MemberInfo,
                        m.Type,
                        Name: CreateNameResolution(m.MemberInfo, m.Attribute),
                        m.Attribute.Order
                    )))
                .OrderBy(m => m.Order)
                .ThenBy(m => m.Name.Value)
                .Select(m => new FieldResolution(m.MemberInfo, m.Type, m.Name))
                .ToList();

            return new RecordResolution(type, name, @namespace, fields);
        }
    }
}
