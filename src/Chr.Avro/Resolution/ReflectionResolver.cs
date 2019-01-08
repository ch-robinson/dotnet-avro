using Chr.Avro.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// A type resolver that relies solely on <see cref="System.Type" /> methods. It’s configured
    /// with a reasonable set of cases that cover most common scenarios.
    /// </summary>
    public class ReflectionResolver : TypeResolver
    {
        /// <summary>
        /// Creates a new reflection resolver.
        /// </summary>
        public ReflectionResolver()
        {
            var additional = new ITypeResolverCase[]
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
                new EnumResolverCase(),

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
                new ObjectResolverCase()
            };

            Cases = additional.Concat(Cases).ToList();
        }
    }

    /// <summary>
    /// An <see cref="ITypeResolverCase" /> that gets its information from type reflection.
    /// </summary>
    public abstract class ReflectionResolverCase : TypeResolverCase { }

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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="bool" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(bool))
            {
                throw new ArgumentException($"The boolean case can only be applied to {typeof(bool).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="byte" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(byte))
            {
                throw new ArgumentException($"The byte case can only be applied to {typeof(byte).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="T:System.Byte[]" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(byte[]))
            {
                throw new ArgumentException($"The byte array case can only be applied to {typeof(byte[]).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(DateTime) && type != typeof(DateTimeOffset))
            {
                throw new ArgumentException($"The date/time case can only be applied to {typeof(DateTime).FullName} or {typeof(DateTimeOffset).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="decimal" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(decimal))
            {
                throw new ArgumentException($"The decimal case can only be applied to {typeof(decimal).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            var pair = type.GetEnumerableType();

            if (pair == null || !pair.IsGenericType || pair.GetGenericTypeDefinition() != typeof(KeyValuePair<,>))
            {
                throw new ArgumentException($"The dictionary case can only be applied to {typeof(IEnumerable<>).MakeGenericType(typeof(KeyValuePair<,>)).FullName}.", nameof(type));
            }

            var parameters = pair.GetGenericArguments();
            var key = parameters.ElementAt(0);
            var value = parameters.ElementAt(1);

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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="double" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(double))
            {
                throw new ArgumentException($"The double case can only be applied to {typeof(double).FullName}.", nameof(type));
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
        public override TypeResolution Apply(Type type)
        {
            if (!type.IsEnum)
            {
                throw new ArgumentException("The enum case can only be applied to enum types.", nameof(type));
            }

            var name = new IdentifierResolution(type.Name);

            var @namespace = string.IsNullOrEmpty(type.Namespace)
                ? null
                : new IdentifierResolution(type.Namespace);

            var hasFlagsAttribute = type.GetCustomAttributes(typeof(FlagsAttribute), true)
                .Any();

            var symbols = type.GetFields(BindingFlags.Public | BindingFlags.Static)
                .Select(f =>
                {
                    // FieldInfo.GetRawConstantValue is arguably nicer than Enum.Parse, but it’s
                    // easier to sort and test the enum values than the underlying constants:
                    var value = Enum.Parse(type, f.Name);

                    return new SymbolResolution(f, new IdentifierResolution(f.Name), value);
                })
                .OrderBy(s => s.Value)
                .ThenBy(s => s.Name.Value)
                .ToList();

            return new EnumResolution(type, name, @namespace, hasFlagsAttribute, symbols);
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="IEnumerable{T}" />.
    /// </summary>
    public class EnumerableResolverCase : ReflectionResolverCase
    {
        /// <summary>
        /// Resolves enumerable type information.
        /// </summary>
        /// <param name="type">
        /// The type to resolve.
        /// </param>
        /// <returns>
        /// An <see cref="ArrayResolution" /> with information about the type.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="IEnumerable{T}" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            var item = type.GetEnumerableType();

            if (item == null)
            {
                throw new ArgumentException($"The enumerable case can only be applied to {typeof(IEnumerable<>).FullName}.", nameof(type));
            }

            return new ArrayResolution(type, type.GetEnumerableType());
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="Guid" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(Guid))
            {
                throw new ArgumentException($"The GUID case can only be applied to {typeof(Guid).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="short" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(short))
            {
                throw new ArgumentException($"The int16 case can only be applied to {typeof(short).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="int" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(int))
            {
                throw new ArgumentException($"The int32 case can only be applied to {typeof(int).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="long" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(long))
            {
                throw new ArgumentException($"The int64 case can only be applied to {typeof(long).FullName}.", nameof(type));
            }

            return new IntegerResolution(type, true, 64);
        }
    }

    /// <summary>
    /// A type resolver case that matches <see cref="Nullable{T}" />.
    /// </summary>
    public class NullableResolverCase : ReflectionResolverCase
    {
        private ITypeResolver resolver;

        /// <summary>
        /// The resolver instance to use to resolve underlying types.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the resolver is set to null.
        /// </exception>
        public ITypeResolver Resolver
        {
            get
            {
                return resolver ?? throw new InvalidOperationException();
            }
            set
            {
                resolver = value ?? throw new ArgumentNullException(nameof(value), "Nullable resolution depends on a resolver instance.");
            }
        }

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
            Resolver = resolver;
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="Nullable{T}" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            var underlying = Nullable.GetUnderlyingType(type);

            if (underlying == null)
            {
                throw new ArgumentException($"The nullable case can only be applied to {typeof(Nullable<>).FullName}.", nameof(type));
            }

            var resolution = Resolver.ResolveType(underlying);
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
        /// The binding flags that will be used to select fields and properties. Only public instance
        /// member are selected by default.
        /// </summary>
        public BindingFlags MemberVisibility { get; set; }

        /// <summary>
        /// Creates a new object resolver case.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags that will be used to select fields and properties. If none are provided,
        /// public instance members will be selected by default.
        /// </param>
        public ObjectResolverCase(BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is an array type or a primitive type.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type.IsArray || type.IsPrimitive)
            {
                throw new ArgumentException("The object resolver can only be applied to non-array, non-primitive types.", nameof(type));
            }

            var name = new IdentifierResolution(type.Name);

            var @namespace = string.IsNullOrEmpty(type.Namespace)
                ? null
                : new IdentifierResolution(type.Namespace);

            var fields = Enumerable
                .Concat(
                    type.GetFields(MemberVisibility).Select(f => (f as MemberInfo, f.FieldType)),
                    type.GetProperties(MemberVisibility).Select(p => (p as MemberInfo, p.PropertyType))
                )
                .Select(r => new FieldResolution(r.Item1, r.Item2, new IdentifierResolution(r.Item1.Name)))
                .OrderBy(f => f.Name.Value)
                .ToList();

            return new RecordResolution(type, name, @namespace, fields);
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="sbyte" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(sbyte))
            {
                throw new ArgumentException($"The sbyte case can only be applied to {typeof(sbyte).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="float" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(float))
            {
                throw new ArgumentException($"The float case can only be applied to {typeof(float).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="string" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(string))
            {
                throw new ArgumentException($"The string case can only be applied to {typeof(string).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="TimeSpan" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(TimeSpan))
            {
                throw new ArgumentException($"The timespan case can only be applied to {typeof(TimeSpan).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="char" /> or <see cref="ushort" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(char) && type != typeof(ushort))
            {
                throw new ArgumentException($"The uint16 case can only be applied to {typeof(char).FullName} or {typeof(ushort).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="uint" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(uint))
            {
                throw new ArgumentException($"The uint32 case can only be applied to {typeof(uint).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="ulong" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(ulong))
            {
                throw new ArgumentException($"The uint64 case can only be applied to {typeof(ulong).FullName}.", nameof(type));
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
        /// <exception cref="ArgumentException">
        /// Thrown when the type is not <see cref="Uri" />.
        /// </exception>
        public override TypeResolution Apply(Type type)
        {
            if (type != typeof(Uri))
            {
                throw new ArgumentException($"The URI case can only be applied to {typeof(Uri).FullName}.", nameof(type));
            }

            return new UriResolution(type);
        }
    }
}
