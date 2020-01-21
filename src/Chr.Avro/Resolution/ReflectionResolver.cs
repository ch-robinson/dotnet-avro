using Chr.Avro.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// A type resolver that relies solely on <see cref="Type" /> methods. It’s configured with a
    /// reasonable set of cases that cover most common scenarios.
    /// </summary>
    public class ReflectionResolver : TypeResolver
    {
        /// <summary>
        /// Creates a new reflection resolver.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select fields and properties. If none are provided,
        /// public instance members will be selected by default.
        /// </param>
        /// <param name="resolveReferenceTypesAsNullable">
        /// Whether to resolve reference types as nullable.
        /// </param>
        /// <param name="resolveUnderlyingEnumTypes">
        /// Whether to resolve enum types as their underlying integral types.
        /// </param>
        public ReflectionResolver(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance,
            bool resolveReferenceTypesAsNullable = false,
            bool resolveUnderlyingEnumTypes = false
        ) : base(CreateReflectionCaseBuilders(memberVisibility, resolveUnderlyingEnumTypes), resolveReferenceTypesAsNullable) { }

        /// <summary>
        /// Creates a default list of case builders.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select fields and properties.
        /// </param>
        /// <param name="resolveUnderlyingEnumTypes">
        /// Whether to resolve enum types as their underlying integral types.
        /// </param>
        public static IEnumerable<Func<ITypeResolver, ITypeResolverCase>> CreateReflectionCaseBuilders(BindingFlags memberVisibility, bool resolveUnderlyingEnumTypes)
        {
            return new Func<ITypeResolver, ITypeResolverCase>[]
            {
                // nullables:
                resolver => new NullableResolverCase(resolver),

                // primitives:
                resolver => new BooleanResolverCase(),
                resolver => new ByteResolverCase(),
                resolver => new ByteArrayResolverCase(),
                resolver => new DecimalResolverCase(),
                resolver => new DoubleResolverCase(),
                resolver => new SingleResolverCase(),
                resolver => new Int16ResolverCase(),
                resolver => new Int32ResolverCase(),
                resolver => new Int64ResolverCase(),
                resolver => new SByteResolverCase(),
                resolver => new StringResolverCase(),
                resolver => new UInt16ResolverCase(),
                resolver => new UInt32ResolverCase(),
                resolver => new UInt64ResolverCase(),

                // enums:
                resolver => resolveUnderlyingEnumTypes
                    ? new EnumUnderlyingTypeResolverCase(resolver)
                    : new EnumResolverCase() as ITypeResolverCase,

                // dictionaries:
                resolver => new DictionaryResolverCase(memberVisibility),

                // enumerables:
                resolver => new EnumerableResolverCase(memberVisibility),

                // built-ins:
                resolver => new DateTimeResolverCase(),
                resolver => new GuidResolverCase(),
                resolver => new TimeSpanResolverCase(),
                resolver => new UriResolverCase(),

                // classes and structs:
                resolver => new ObjectResolverCase(memberVisibility)
            };
        }
    }

    /// <summary>
    /// An <see cref="ITypeResolverCase" /> that gets its information from type reflection.
    /// </summary>
    public abstract class ReflectionResolverCase : TypeResolverCase
    {
        /// <summary>
        /// Gets an attribute on a type member.
        /// </summary>
        /// <returns>
        /// The attribute, or null if the attribute is not present.
        /// </returns>
        protected virtual T? GetAttribute<T>(MemberInfo member) where T : Attribute
        {
            return member.GetCustomAttributes(typeof(T), true)
                .OfType<T>()
                .SingleOrDefault();
        }

        /// <summary>
        /// Gets an attribute on a type.
        /// </summary>
        /// <returns>
        /// The attribute, or null if the attribute is not present.
        /// </returns>
        protected virtual T? GetAttribute<T>(Type type) where T : Attribute
        {
            return type.GetCustomAttributes(typeof(T), true)
                .OfType<T>()
                .SingleOrDefault();
        }

        /// <summary>
        /// Gets constructors on a type.
        /// </summary>
        /// <returns>
        /// The constructors and their parameters.
        /// </returns>
        protected virtual IEnumerable<(ConstructorInfo ConstructorInfo, IEnumerable<ParameterInfo> Parameters)> GetConstructors(Type type, BindingFlags visibility)
        {
            return type.GetConstructors(visibility)
                .Select(c => (c, c.GetParameters() as IEnumerable<ParameterInfo>));
        }

        /// <summary>
        /// Gets fields an properties on a type.
        /// </summary>
        protected virtual IEnumerable<(MemberInfo MemberInfo, Type Type)> GetMembers(Type type, BindingFlags visibility)
        {
            return Enumerable
                .Concat(
                    type.GetFields(visibility).Select(f => (f as MemberInfo, f.FieldType)),
                    type.GetProperties(visibility).Select(p => (p as MemberInfo, p.PropertyType))
                );
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="bool" />.
    /// </summary>
    public class BooleanResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves boolean type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="BooleanResolution" /> result if the type is <see cref="bool" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(bool))
            {
                result.TypeResolution = new BooleanResolution(type);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="byte" />.
    /// </summary>
    public class ByteResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves byte (8-bit unsigned integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="byte" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(byte))
            {
                result.TypeResolution = new IntegerResolution(type, false, 8);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="T:System.Byte[]" />.
    /// </summary>
    public class ByteArrayResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves byte array type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="ByteArrayResolution" /> result if the type is <see cref="T:System.Byte[]" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(byte[]))
            {
                result.TypeResolution = new ByteArrayResolution(type);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="DateTime" /> and <see cref="DateTimeOffset" />.
    /// </summary>
    public class DateTimeResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves date/time type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="TimestampResolution" /> result if the type is <see cref="DateTime" />
        /// or <see cref="DateTimeOffset" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(DateTime) || type == typeof(DateTimeOffset))
            {
                result.TypeResolution = new TimestampResolution(type, 1m / TimeSpan.TicksPerSecond);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="decimal" />.
    /// </summary>
    public class DecimalResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves decimal type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="DecimalResolution" /> result if the type is <see cref="decimal" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(decimal))
            {
                result.TypeResolution = new DecimalResolution(type, 29, 14);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
    /// </summary>
    public class DictionaryResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// The binding flags that will be used to select constructors.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Creates a new dictionary resolver case.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select constructors.
        /// </param>
        public DictionaryResolverCase(BindingFlags memberVisibility)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Resolves dictionary type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="MapResolution" /> result if the type is <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type.GetEnumerableType() is Type pair && pair.IsGenericType && pair.GetGenericTypeDefinition() == typeof(KeyValuePair<,>))
            {
                var arguments = pair.GetGenericArguments();
                var key = arguments.ElementAt(0);
                var value = arguments.ElementAt(1);
                var constructors = GetConstructors(type, MemberVisibility)
                    .Select(c => new ConstructorResolution(
                        c.ConstructorInfo,
                        c.Parameters.Select(p => new ParameterResolution(p, p.ParameterType, new IdentifierResolution(p.Name))).ToList()
                    ))
                    .ToList();

                result.TypeResolution = new MapResolution(type, key, value, constructors);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="double" />.
    /// </summary>
    public class DoubleResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves double-precision floating-point type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="FloatingPointResolution" /> result if the type is <see cref="double" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(double))
            {
                result.TypeResolution = new FloatingPointResolution(type, 16);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Enum" /> types.
    /// </summary>
    public class EnumResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves enum type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="EnumResolution" /> result if the type is an <see cref="Enum" /> type;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type.IsEnum)
            {
                var name = new IdentifierResolution(type.Name);

                var @namespace = string.IsNullOrEmpty(type.Namespace)
                    ? null
                    : new IdentifierResolution(type.Namespace);

                var isFlagEnum = GetAttribute<FlagsAttribute>(type) != null;

                var symbols = type.GetFields(BindingFlags.Public | BindingFlags.Static)
                    .Select(f => (
                        MemberInfo: f as MemberInfo,
                        Name: new IdentifierResolution(f.Name),
                        Value: Enum.Parse(type, f.Name)
                    ))
                    .OrderBy(f => f.Value)
                    .ThenBy(f => f.Name.Value)
                    .Select(f => new SymbolResolution(f.MemberInfo, f.Name, f.Value))
                    .ToList();

                result.TypeResolution = new EnumResolution(type, type.GetEnumUnderlyingType(), name, @namespace, isFlagEnum, symbols);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Enum" /> types and resolves underlying integral
    /// types.
    /// </summary>
    public class EnumUnderlyingTypeResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// The resolver instance to use to resolve underlying types.
        /// </summary>
        public ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates a new nullable resolver case.
        /// </summary>
        /// <param name="resolver">
        /// The resolver instance to use to resolve underlying types.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the resolver is null.
        /// </exception>
        public EnumUnderlyingTypeResolverCase(ITypeResolver resolver)
        {
            Resolver = resolver ?? throw new ArgumentNullException(nameof(resolver), "Resolver cannot be null.");
        }

        /// <summary>
        /// Resolves underlying enum type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// If the type is an <see cref="Enum" /> type, a successful <see cref="TypeResolution" />
        /// for its underlying type; an unsuccessful <see cref="UnsupportedTypeException" /> result
        /// otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type.IsEnum)
            {
                var resolution = Resolver.ResolveType(type.GetEnumUnderlyingType());
                resolution.Type = type;

                result.TypeResolution = resolution;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="IEnumerable{T}" />.
    /// </summary>
    public class EnumerableResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// The binding flags that will be used to select constructors.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Creates a new enumerable resolver case.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select constructors.
        /// </param>
        public EnumerableResolverCase(BindingFlags memberVisibility)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Resolves enumerable type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="ArrayResolution" /> result if the type is <see cref="IEnumerable{T}" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type.GetEnumerableType() is Type itemType)
            {
                var resolution = new ArrayResolution(type, itemType);

                if (!type.IsArray)
                {
                    resolution.Constructors = GetConstructors(type, MemberVisibility)
                        .Select(c => new ConstructorResolution(
                            c.ConstructorInfo,
                            c.Parameters.Select(p => new ParameterResolution(p, p.ParameterType, new IdentifierResolution(p.Name))).ToList()
                        ))
                        .ToList();
                }

                result.TypeResolution = resolution;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Guid" />.
    /// </summary>
    public class GuidResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves GUID type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="UuidResolution" /> result if the type is <see cref="Guid" />; an
        /// unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(Guid))
            {
                result.TypeResolution = new UuidResolution(type, 2, 4);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="short" />.
    /// </summary>
    public class Int16ResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves short (16-bit signed integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="short" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(short))
            {
                result.TypeResolution = new IntegerResolution(type, true, 16);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="int" />.
    /// </summary>
    public class Int32ResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves int (32-bit signed integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="int" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(int))
            {
                result.TypeResolution = new IntegerResolution(type, true, 32);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="long" />.
    /// </summary>
    public class Int64ResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves long (64-bit signed integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="long" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(long))
            {
                result.TypeResolution = new IntegerResolution(type, true, 64);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Nullable{T}" />.
    /// </summary>
    public class NullableResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// The resolver instance to use to resolve underlying types.
        /// </summary>
        public ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates a new nullable resolver case.
        /// </summary>
        /// <param name="resolver">
        /// The resolver instance to use to resolve underlying types.
        /// </param>
        public NullableResolverCase(ITypeResolver resolver)
        {
            Resolver = resolver;
        }

        /// <summary>
        /// Resolves nullable type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// If the type is <see cref="Nullable{T}" />, a successful <see cref="TypeResolution" />
        /// for its underlying type; an unsuccessful <see cref="UnsupportedTypeException" /> result
        /// otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (Nullable.GetUnderlyingType(type) is Type underlyingType)
            {
                var resolution = Resolver.ResolveType(underlyingType);
                resolution.IsNullable = true;
                resolution.Type = type;

                result.TypeResolution = resolution;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A general type resolver case that inspects fields and properties.
    /// </summary>
    public class ObjectResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// The binding flags that will be used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Creates a new object resolver case.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select fields and properties.
        /// </param>
        public ObjectResolverCase(BindingFlags memberVisibility)
        {
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Resolves class, interface, or struct type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// An unsuccessful <see cref="UnsupportedTypeException" /> result if the type is an array
        /// or primitive type; a successful <see cref="RecordResolution" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (!type.IsArray && !type.IsPrimitive)
            {
                var name = new IdentifierResolution(type.Name);
                var @namespace = string.IsNullOrEmpty(type.Namespace)
                    ? null
                    : new IdentifierResolution(type.Namespace);

                var fields = GetMembers(type, MemberVisibility)
                    .Select(m => new FieldResolution(
                        m.MemberInfo,
                        m.Type,
                        new IdentifierResolution(m.MemberInfo.Name)
                    ))
                    .OrderBy(m => m.Name.Value)
                    .ToList();

                var constructors = GetConstructors(type, MemberVisibility)
                    .Select(c => new ConstructorResolution(
                        c.ConstructorInfo,
                        c.Parameters.Select(p => new ParameterResolution(p, p.ParameterType, new IdentifierResolution(p.Name))).ToList()
                    ))
                    .ToList();

                result.TypeResolution = new RecordResolution(type, name, @namespace, fields, constructors);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="sbyte" />.
    /// </summary>
    public class SByteResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves sbyte (8-bit signed integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="sbyte" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(sbyte))
            {
                result.TypeResolution = new IntegerResolution(type, true, 8);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="float" />.
    /// </summary>
    public class SingleResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves single-precision floating-point type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="FloatingPointResolution" /> result if the type is <see cref="float" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(float))
            {
                result.TypeResolution = new FloatingPointResolution(type, 8);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver that matches <see cref="string" />.
    /// </summary>
    public class StringResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves string type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="StringResolution" /> result if the type is <see cref="string" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(string))
            {
                result.TypeResolution = new StringResolution(type);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver that matches <see cref="TimeSpan" />.
    /// </summary>
    public class TimeSpanResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves duration type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="DurationResolution" /> result if the type is <see cref="TimeSpan" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(TimeSpan))
            {
                result.TypeResolution = new DurationResolution(type, 1m / TimeSpan.TicksPerSecond);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="char" /> and <see cref="ushort" />.
    /// </summary>
    public class UInt16ResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves ushort (16-bit unsigned integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="char" />
        /// or <see cref="ushort" />; an unsuccessful <see cref="UnsupportedTypeException" /> result
        /// otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(char) || type == typeof(ushort))
            {
                result.TypeResolution = new IntegerResolution(type, false, 16);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="uint" />.
    /// </summary>
    public class UInt32ResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves uint (32-bit unsigned integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="uint" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(uint))
            {
                result.TypeResolution = new IntegerResolution(type, false, 32);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="ulong" />.
    /// </summary>
    public class UInt64ResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves ulong (64-bit unsigned integer) type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="IntegerResolution" /> result if the type is <see cref="ulong" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(ulong))
            {
                result.TypeResolution = new IntegerResolution(type, false, 64);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Uri" />.
    /// </summary>
    public class UriResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves URI type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A successful <see cref="UriResolution" /> result if the type is <see cref="Uri" />;
        /// an unsuccessful <see cref="UnsupportedTypeException" /> result otherwise.
        /// </returns>
        public override ITypeResolutionResult ResolveType(Type type)
        {
            var result = new TypeResolutionResult();

            if (type == typeof(Uri))
            {
                result.TypeResolution = new UriResolution(type);
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(type));
            }

            return result;
        }
    }
}
