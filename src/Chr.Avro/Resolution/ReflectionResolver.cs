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
                resolver => new DictionaryResolverCase(),

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
        protected virtual T GetAttribute<T>(MemberInfo member) where T : Attribute
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
        protected virtual T GetAttribute<T>(Type type) where T : Attribute
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
        /// A <see cref="BooleanResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="bool" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(bool))
            {
                throw new UnsupportedTypeException(type);
            }

            return new BooleanResolution(type);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="byte" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(byte))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, false, 8);
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
        /// A <see cref="ByteArrayResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="T:System.Byte[]" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(byte[]))
            {
                throw new UnsupportedTypeException(type);
            }

            return new ByteArrayResolution(type);
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
        /// A <see cref="TimestampResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!(type == typeof(DateTime) || type == typeof(DateTimeOffset)))
            {
                throw new UnsupportedTypeException(type);
            }

            return new TimestampResolution(type, 1m / TimeSpan.TicksPerSecond);
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
        /// A <see cref="DecimalResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="decimal" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(decimal))
            {
                throw new UnsupportedTypeException(type);
            }

            return new DecimalResolution(type, 29, 14);
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
    /// </summary>
    public class DictionaryResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves dictionary type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A <see cref="MapResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!(type.GetEnumerableType() is Type pair && pair.IsGenericType && pair.GetGenericTypeDefinition() == typeof(KeyValuePair<,>)))
            {
                throw new UnsupportedTypeException(type);
            }

            var arguments = pair.GetGenericArguments();
            var key = arguments.ElementAt(0);
            var value = arguments.ElementAt(1);

            return new MapResolution(type, key, value);
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
        /// A <see cref="FloatingPointResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="double" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(double))
            {
                throw new UnsupportedTypeException(type);
            }

            return new FloatingPointResolution(type, 16);
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Enum" /> types.
    /// </summary>
    public class EnumResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// The binding flags that will be used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }



        /// <summary>
        /// Resolves enum type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// An <see cref="EnumResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not an enum type.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!type.IsEnum)
            {
                throw new UnsupportedTypeException(type);
            }

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

            return new EnumResolution(type, type.GetEnumUnderlyingType(), name, @namespace, isFlagEnum, symbols);
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
        /// A <see cref="TypeResolution" /> for the underlying type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not an enum type.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!type.IsEnum)
            {
                throw new UnsupportedTypeException(type);
            }

            var resolution = Resolver.ResolveType(type.GetEnumUnderlyingType());
            resolution.Type = type;

            return resolution;
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="IEnumerable{T}" />.
    /// </summary>
    public class EnumerableResolverCase : ReflectionResolverCase
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
        /// An <see cref="ArrayResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="IEnumerable{T}" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            var itemType = type.GetEnumerableType();

            if (itemType == null)
            {
                throw new UnsupportedTypeException(type);
            }

            ICollection<ConstructorResolution> constructors = null;

            if (!type.IsArray)
            {
                constructors = GetConstructors(type, MemberVisibility)
                    .Select(c => new ConstructorResolution(
                        c.ConstructorInfo,
                        c.Parameters.Select(p => new ParameterResolution(p, p.ParameterType, new IdentifierResolution(p.Name))).ToList()
                    )).ToList();
            }

            return new ArrayResolution(type, itemType, constructors);
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
        /// A <see cref="UuidResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="Guid" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(Guid))
            {
                throw new UnsupportedTypeException(type);
            }

            return new UuidResolution(type, 2, 4);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="short" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(short))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, true, 16);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="int" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(int))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, true, 32);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="long" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(long))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, true, 64);
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
        /// <exception cref="ArgumentNullException">
        /// Thrown when the resolver is null.
        /// </exception>
        public NullableResolverCase(ITypeResolver resolver)
        {
            Resolver = resolver ?? throw new ArgumentNullException(nameof(resolver), "Resolver cannot be null.");
        }

        /// <summary>
        /// Resolves nullable type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// A <see cref="TypeResolution" /> for the underlying type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="Nullable{T}" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);

            if (underlyingType == null)
            {
                throw new UnsupportedTypeException(type);
            }

            var resolution = Resolver.ResolveType(underlyingType);
            resolution.IsNullable = true;
            resolution.Type = type;

            return resolution;
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
        /// A <see cref="RecordResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is an array type or a primitive type.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type.IsArray || type.IsPrimitive)
            {
                throw new UnsupportedTypeException(type);
            }

            var name = new IdentifierResolution(type.Name);

            var @namespace = string.IsNullOrEmpty(type.Namespace)
                ? null
                : new IdentifierResolution(type.Namespace);

            var fields = GetMembers(type, MemberVisibility)
                .Select(m => (
                    m.MemberInfo,
                    m.Type,
                    Name: new IdentifierResolution(m.MemberInfo.Name)
                ))
                .OrderBy(m => m.Name.Value)
                .Select(m => new FieldResolution(m.MemberInfo, m.Type, m.Name))
                .ToList();

            var constructors = GetConstructors(type, MemberVisibility)
                .Select(c => new ConstructorResolution(
                    c.ConstructorInfo,
                    c.Parameters.Select(p => new ParameterResolution(p, p.ParameterType, new IdentifierResolution(p.Name))).ToList()
                )).ToList();

            return new RecordResolution(type, name, @namespace, fields, constructors);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="sbyte" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(sbyte))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, true, 8);
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
        /// A <see cref="FloatingPointResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="float" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(float))
            {
                throw new UnsupportedTypeException(type);
            }

            return new FloatingPointResolution(type, 8);
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
        /// A <see cref="StringResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="string" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(string))
            {
                throw new UnsupportedTypeException(type);
            }

            return new StringResolution(type);
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
        /// A <see cref="DurationResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="TimeSpan" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(TimeSpan))
            {
                throw new UnsupportedTypeException(type);
            }

            return new DurationResolution(type, 1m / TimeSpan.TicksPerSecond);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="char" /> or <see cref="ushort" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (!(type == typeof(char) || type == typeof(ushort)))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, false, 16);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="uint" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(uint))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, false, 32);
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
        /// An <see cref="IntegerResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="ulong" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(ulong))
            {
                throw new UnsupportedTypeException(type);
            }

            return new IntegerResolution(type, false, 64);
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
        /// A <see cref="UriResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="Uri" />.
        /// </exception>
        public override TypeResolution Resolve(Type type)
        {
            if (type != typeof(Uri))
            {
                throw new UnsupportedTypeException(type);
            }

            return new UriResolution(type);
        }
    }
}
