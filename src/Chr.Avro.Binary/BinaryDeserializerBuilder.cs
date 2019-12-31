using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Xml;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Builds Avro deserializers for .NET types.
    /// </summary>
    public interface IBinaryDeserializerBuilder
    {
        /// <summary>
        /// Builds a delegate that reads a serialized object from a stream.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized. If the type is a class or a struct, it must have
        /// a parameterless public constructor.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// An optional delegate cache. The cache can be used to provide custom implementations for
        /// particular type-schema pairs, and it will also be populated as the delegate is built.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        Func<Stream, T> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache = null);

        /// <summary>
        /// Builds a binary deserializer.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized. If the type is a class or a struct, it must have
        /// a parameterless public constructor.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        IBinaryDeserializer<T> BuildDeserializer<T>(Schema schema);
    }

    /// <summary>
    /// Builds Avro deserializers for specific type-schema combinations. Used by
    /// <see cref="BinaryDeserializerBuilder" /> to break apart deserializer building logic.
    /// </summary>
    public interface IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache. If a delegate is cached for a specific type-schema pair, that delegate
        /// will be returned for all subsequent occurrences of the pair.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object. Since
        /// this is not a typed method, the general <see cref="Delegate" /> type is used.
        /// </returns>
        Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);
    }

    /// <summary>
    /// A deserializer builder configured with a reasonable set of default cases.
    /// </summary>
    public class BinaryDeserializerBuilder : IBinaryDeserializerBuilder
    {
        /// <summary>
        /// A list of cases that the build methods will attempt to apply. If the first case does
        /// not match, the next case will be tested, and so on.
        /// </summary>
        public IEnumerable<IBinaryDeserializerBuilderCase> Cases { get; }

        /// <summary>
        /// A resolver to obtain type information from.
        /// </summary>
        public ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates a new deserializer builder.
        /// </summary>
        /// <param name="codec">
        /// A codec implementation that generated deserializers will use for read operations. If
        /// no codec is provided, <see cref="BinaryCodec" /> will be used.
        /// </param>
        /// <param name="resolver">
        /// A resolver to obtain type information from.
        /// </param>
        public BinaryDeserializerBuilder(IBinaryCodec codec = null, ITypeResolver resolver = null)
            : this(CreateBinaryDeserializerCaseBuilders(codec ?? new BinaryCodec()), resolver) { }

        /// <summary>
        /// Creates a new deserializer builder.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolver">
        /// A resolver to obtain type information from.
        /// </param>
        public BinaryDeserializerBuilder(IEnumerable<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> caseBuilders, ITypeResolver resolver = null)
        {
            var cases = new List<IBinaryDeserializerBuilderCase>();

            Cases = cases;
            Resolver = resolver ?? new DataContractResolver();

            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Builds a delegate that reads a serialized object from a stream.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized. If the type is a class or a struct, it must have
        /// a parameterless public constructor.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// An optional delegate cache. The cache can be used to provide custom implementations for
        /// particular type-schema pairs, and it will also be populated as the delegate is built.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="AggregateException">
        /// Thrown when no case matches the schema or type. <see cref="AggregateException.InnerExceptions" />
        /// will be contain the exceptions thrown by each case.
        /// </exception>
        public virtual Func<Stream, T> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache = null)
        {
            if (cache == null)
            {
                cache = new ConcurrentDictionary<(Type, Schema), Delegate>();
            }

            var resolution = Resolver.ResolveType(typeof(T));

            if (cache.TryGetValue((resolution.Type, schema), out var existing))
            {
                return existing as Func<Stream, T>;
            }

            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                try
                {
                    return @case.BuildDelegate(resolution, schema, cache) as Func<Stream, T>;
                }
                catch (UnsupportedSchemaException exception)
                {
                    exceptions.Add(exception);
                }
                catch (UnsupportedTypeException exception)
                {
                    exceptions.Add(exception);
                }
            }

            throw new AggregateException($"No deserializer builder case matched {resolution.GetType().Name}.", exceptions);
        }

        /// <summary>
        /// Builds a binary deserializer.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized. If the type is a class or a struct, it must have
        /// a parameterless public constructor.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <exception cref="AggregateException">
        /// Thrown when no case matches the schema or type. <see cref="AggregateException.InnerExceptions" />
        /// will be contain the exceptions thrown by each case.
        /// </exception>
        public virtual IBinaryDeserializer<T> BuildDeserializer<T>(Schema schema)
        {
            return new BinaryDeserializer<T>(BuildDelegate<T>(schema));
        }

        /// <summary>
        /// Creates a default list of case builders.
        /// </summary>
        /// <param name="codec">
        /// A codec implementation that generated deserializers will use for read operations.
        /// </param>
        public static IEnumerable<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> CreateBinaryDeserializerCaseBuilders(IBinaryCodec codec)
        {
            return new Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>[]
            {
                // logical types:
                builder => new DecimalDeserializerBuilderCase(codec),
                builder => new DurationDeserializerBuilderCase(codec),
                builder => new TimestampDeserializerBuilderCase(codec),

                // primitives:
                builder => new BooleanDeserializerBuilderCase(codec),
                builder => new BytesDeserializerBuilderCase(codec),
                builder => new DoubleDeserializerBuilderCase(codec),
                builder => new FixedDeserializerBuilderCase(codec),
                builder => new FloatDeserializerBuilderCase(codec),
                builder => new IntegerDeserializerBuilderCase(codec),
                builder => new NullDeserializerBuilderCase(),
                builder => new StringDeserializerBuilderCase(codec),

                // collections:
                builder => new ArrayDeserializerBuilderCase(codec, builder),
                builder => new MapDeserializerBuilderCase(codec, builder),

                // enums:
                builder => new EnumDeserializerBuilderCase(codec),

                // records:
                builder => new RecordConstructorDeserializerBuilderCase(builder),
                builder => new RecordDeserializerBuilderCase(builder),

                // unions:
                builder => new UnionDeserializerBuilderCase(codec, builder)
            };
        }
    }

    /// <summary>
    /// A base deserializer builder case.
    /// </summary>
    public abstract class BinaryDeserializerBuilderCase : IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache. If a delegate is cached for a specific type-schema pair, that delegate
        /// will be returned for all subsequent occurrences of the pair.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object. Since
        /// this is not a typed method, the general <see cref="Delegate" /> type is used.
        /// </returns>
        public abstract Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="ArraySchema" /> and attempts to map it
    /// to enumerable types.
    /// </summary>
    public class ArrayDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// The deserializer builder to use to build item deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new array deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build item deserializers.
        /// </param>
        public ArrayDeserializerBuilderCase(IBinaryCodec codec, IBinaryDeserializerBuilder deserializerBuilder)
        {
            Codec = codec;
            DeserializerBuilder = deserializerBuilder;
        }

        /// <summary>
        /// Builds an array deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an <see cref="ArraySchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type is neither an array type nor a type assignable from
        /// <see cref="List{T}" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is ArrayResolution arrayResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "An array deserializer can only be built for an array resolution.");
            }

            if (!(schema is ArraySchema arraySchema))
            {
                throw new UnsupportedSchemaException(schema, "An array deserializer can only be built for an array schema.");
            }

            var target = arrayResolution.Type;
            var item = arrayResolution.ItemType;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            Expression result = null;

            try
            {
                var build = typeof(IBinaryDeserializerBuilder)
                    .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate))
                    .MakeGenericMethod(item);

                var readBlocks = typeof(IBinaryCodec)
                    .GetMethods()
                    .Single(m => m.Name == nameof(IBinaryCodec.ReadBlocks)
                        && m.GetGenericArguments().Length == 1
                    )
                    .MakeGenericMethod(item);

                result = Expression.Call(
                    codec,
                    readBlocks,
                    stream,
                    Expression.Constant(
                        build.Invoke(DeserializerBuilder, new object[] { arraySchema.Item, cache }),
                        typeof(Func<,>).MakeGenericType(typeof(Stream), item)
                    )
                );
            }
            catch (TargetInvocationException indirect)
            {
                ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
            }

            var constructor = FindEnumerableConstructor(arrayResolution, item);

            if (constructor != null)
            {
                var value = Expression.Parameter(target);
                result = Expression.Block(
                    new[] { value },
                    Expression.Assign(value, Expression.New(constructor.Constructor, new[] { result })),
                    value
                );
            }
            else
            {
                var convert = typeof(Enumerable).GetMethods()
                    .Where(m => m.Name == (target.IsArray
                        ? nameof(Enumerable.ToArray)
                        : nameof(Enumerable.ToList)
                    ))
                    .Single()
                    .MakeGenericMethod(item);

                if (!target.IsAssignableFrom(convert.ReturnType))
                {
                    throw new UnsupportedTypeException(target, $"An array deserializer cannot be built for type {target.FullName}.");
                }

                result = Expression.ConvertChecked(Expression.Call(null, convert, result), target);
            }

            var lambda = Expression.Lambda(result, "array deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }

        private ConstructorResolution FindEnumerableConstructor(ArrayResolution arrayResolution, Type type)
        {
            ConstructorResolution match = null;
            foreach (var constructor in arrayResolution.Constructors)
            {
                if (constructor.Parameters.Count == 1)
                {
                    var parameterType = constructor.Parameters.First().Type;
                    if (parameterType.IsGenericType && parameterType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    {
                        var arguments = parameterType.GetGenericArguments();
                        if (arguments.Count() == 1 && arguments[0] == type)
                        {
                            match = constructor;
                            break;
                        }
                    }
                }
            }

            return match;
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="BooleanSchema" /> and attempts to map
    /// it to any provided type.
    /// </summary>
    public class BooleanDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new boolean deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public BooleanDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a boolean deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BooleanSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="bool" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is BooleanSchema))
            {
                throw new UnsupportedSchemaException(schema, "A boolean deserializer can only be built for a boolean schema.");
            }

            var source = typeof(bool);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadBoolean));

            Expression result = Expression.Call(codec, readValue, stream);

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A boolean deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "boolean deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="BytesSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class BytesDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new variable-length bytes deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public BytesDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a variable-length bytes deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BytesSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="T:System.Byte[]" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is BytesSchema))
            {
                throw new UnsupportedSchemaException(schema, "A bytes deserializer can only be built for a bytes schema.");
            }

            var source = typeof(byte[]);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readLength = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadInteger));

            Expression result = Expression.ConvertChecked(Expression.Call(codec, readLength, stream), typeof(int));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Read));

            result = Expression.Call(codec, readValue, stream, result);

            if (source != target)
            {
                if (target == typeof(Guid) || target == typeof(Guid?))
                {
                    var guidConstructor = typeof(Guid)
                        .GetConstructor(new[] { typeof(byte[]) });

                    result = Expression.New(guidConstructor, result);
                }

                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A bytes deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "bytes deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="DecimalLogicalType" /> and attempts to
    /// map it to any provided type.
    /// </summary>
    public class DecimalDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new decimal deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public DecimalDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a decimal deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BytesSchema" /> or a <see cref="FixedSchema "/>
        /// with logical type <see cref="DecimalLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="decimal" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema.LogicalType is DecimalLogicalType decimalLogicalType))
            {
                throw new UnsupportedSchemaException(schema, "A decimal deserializer can only be built for schema with a decimal logical type.");
            }

            var precision = decimalLogicalType.Precision;
            var scale = decimalLogicalType.Scale;
            var source = typeof(decimal);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            Expression result;

            // figure out the size:
            if (schema is BytesSchema)
            {
                var readLength = typeof(IBinaryCodec)
                    .GetMethod(nameof(IBinaryCodec.ReadInteger));

                result = Expression.ConvertChecked(Expression.Call(codec, readLength, stream), typeof(int));
            }
            else if (schema is FixedSchema fixedSchema)
            {
                result = Expression.Constant(fixedSchema.Size);
            }
            else
            {
                throw new UnsupportedSchemaException(schema, "A decimal deserializer can only be built for a bytes or a fixed schema.");
            }

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Read));

            // read the bytes:
            result = Expression.Call(codec, readValue, stream, result);

            // declare some variables for in-place transformation:
            var bytes = Expression.Variable(typeof(byte[]));

            var integerConstructor = typeof(BigInteger)
                .GetConstructor(new[] { typeof(byte[]) });

            var reverse = typeof(Array)
                .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

            result = Expression.Block(
                new[] { bytes },

                // store the bytes in a variable:
                Expression.Assign(bytes, result),

                // BigInteger is little-endian, so reverse before creating:
                Expression.Call(null, reverse, bytes),

                // return (decimal)new BigInteger(bytes) / (decimal)Math.Pow(10, scale);
                Expression.Divide(
                    Expression.ConvertChecked(
                        Expression.New(integerConstructor, bytes),
                        typeof(decimal)),
                    Expression.Constant((decimal)Math.Pow(10, scale)))
            );

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A decimal deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "decimal deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="DoubleSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class DoubleDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new double deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public DoubleDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a double deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="DoubleSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="double" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is DoubleSchema))
            {
                throw new UnsupportedSchemaException(schema, "A double deserializer can only be built for a double schema.");
            }

            var source = typeof(double);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadDouble));

            Expression result = Expression.Call(codec, readValue, stream);

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A double deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "double deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="DurationLogicalType" /> and attempts to
    /// map it to <see cref="TimeSpan" />.
    /// </summary>
    public class DurationDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new duration deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public DurationDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a duration deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FixedSchema" /> with size 12 and logical
        /// type <see cref="DurationLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="TimeSpan" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema.LogicalType is DurationLogicalType))
            {
                throw new UnsupportedSchemaException(schema, "A duration deserializer can only be built for a schema with a duration logical type.");
            }

            if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
            {
                throw new UnsupportedSchemaException(schema, "A duration deserializer can only be built for a fixed schema with size 12.");
            }

            var target = resolution.Type;

            if (!(target == typeof(TimeSpan) || target == typeof(TimeSpan?)))
            {
                throw new UnsupportedTypeException(target, $"A duration deserializer cannot be built for {target.Name}.");
            }

            Func<Stream, long> read = input =>
            {
                var bytes = Codec.Read(input, 4);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                return BitConverter.ToUInt32(bytes, 0);
            };

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            Expression result = Expression.Block(
                Expression.IfThen(
                    Expression.NotEqual(
                        Expression.Invoke(Expression.Constant(read), stream),
                        Expression.Constant(0L)
                    ),
                    Expression.Throw(
                        Expression.New(
                            typeof(OverflowException).GetConstructor(new[] { typeof(string )}),
                            Expression.Constant("Durations containing months cannot be accurately deserialized to a TimeSpan.")
                        )
                    )
                ),
                Expression.ConvertChecked(
                    Expression.New(
                        typeof(TimeSpan).GetConstructor(new[] { typeof(long) }),
                        Expression.AddChecked(
                            Expression.MultiplyChecked(
                                Expression.Invoke(Expression.Constant(read), stream),
                                Expression.Constant(TimeSpan.TicksPerDay)
                            ),
                            Expression.MultiplyChecked(
                                Expression.Invoke(Expression.Constant(read), stream),
                                Expression.Constant(TimeSpan.TicksPerMillisecond)
                            )
                        )
                    ),
                    target
                )
            );

            var lambda = Expression.Lambda(result, $"duration deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="EnumSchema" /> and attempts to map it
    /// to enum types.
    /// </summary>
    public class EnumDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new enum deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public EnumDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds an enum deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an <see cref="EnumSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not an <see cref="EnumResolution" /> or the type does not
        /// contain a matching symbol for each symbol in the schema.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is EnumResolution enumResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "An enum deserializer can only be built for an enum resolution.");
            }

            if (!(schema is EnumSchema enumSchema))
            {
                throw new UnsupportedSchemaException(schema, "An enum deserializer can only be built for an enum schema.");
            }

            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readIndex = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadInteger));

            Expression result = Expression.ConvertChecked(Expression.Call(codec, readIndex, stream), typeof(int));

            // find a match for each enum in the schema:
            var cases = enumSchema.Symbols.Select((name, index) =>
            {
                var match = enumResolution.Symbols.SingleOrDefault(s => s.Name.IsMatch(name));

                if (match == null)
                {
                    throw new UnsupportedTypeException(target, $"{target.Name} has no value that matches {name}.");
                }

                return Expression.SwitchCase(
                    Expression.ConvertChecked(Expression.Constant(match.Value), target),
                    Expression.Constant(index)
                );
            });

            var exceptionConstructor = typeof(OverflowException)
                .GetConstructor(new[] { typeof(string) });

            var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum index out of range."));

            // generate a switch on the index:
            result = Expression.Switch(result, Expression.Throw(exception, target), cases.ToArray());

            var lambda = Expression.Lambda(result, $"{enumSchema.Name} deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="FixedSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class FixedDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new fixed-length bytes deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public FixedDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a fixed-length bytes deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FixedSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="T:System.Byte[]" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is FixedSchema fixedSchema))
            {
                throw new UnsupportedSchemaException(schema, "A fixed deserializer can only be built for a fixed schema.");
            }

            var source = typeof(byte[]);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Read));

            Expression result = Expression.Call(codec, readValue, stream, Expression.Constant(fixedSchema.Size));

            if (source != target)
            {
                if (target == typeof(Guid) || target == typeof(Guid?))
                {
                    if (fixedSchema.Size != 16)
                    {
                        throw new UnsupportedSchemaException(schema, $"A fixed schema cannot be mapped to a Guid unless its size is 16.");
                    }

                    var guidConstructor = typeof(Guid)
                        .GetConstructor(new[] { typeof(byte[]) });

                    result = Expression.New(guidConstructor, result);
                }

                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A fixed deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, $"{fixedSchema.Name} deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="FloatSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class FloatDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new float deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public FloatDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a float deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FloatSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="float" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is FloatSchema))
            {
                throw new UnsupportedSchemaException(schema, "A float deserializer can only be built for a float schema.");
            }

            var source = typeof(float);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadSingle));

            Expression result = Expression.Call(codec, readValue, stream);

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A float deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "float deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="IntSchema" /> or <see cref="LongSchema" />
    /// and attempts to map them to any provided type.
    /// </summary>
    public class IntegerDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new integer deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public IntegerDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds an integer deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an <see cref="IntSchema" /> or a <see cref="LongSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion from <see cref="long" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is IntSchema || schema is LongSchema))
            {
                throw new UnsupportedSchemaException(schema, "An integer deserializer can only be built for an int or long schema.");
            }

            var source = typeof(long);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadInteger));

            Expression result = Expression.Call(codec, readValue, stream);

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"An integer deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "integer deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="MapSchema" /> and attempts to map it
    /// to dictionary types.
    /// </summary>
    public class MapDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// The deserializer builder to use to build key and value deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new map deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build key and value deserializers.
        /// </param>
        public MapDeserializerBuilderCase(IBinaryCodec codec, IBinaryDeserializerBuilder deserializerBuilder)
        {
            Codec = codec;
            DeserializerBuilder = deserializerBuilder;
        }

        /// <summary>
        /// Builds an map deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="MapSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not a <see cref="MapResolution" /> or the resolved type
        /// is not assignable from <see cref="Dictionary{TKey, TValue}" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is MapResolution mapResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "A map deserializer can only be built for a map resolution.");
            }

            if (!(schema is MapSchema mapSchema))
            {
                throw new UnsupportedSchemaException(schema, "A map deserializer can only be built for a map schema.");
            }

            var target = mapResolution.Type;
            var key = mapResolution.KeyType;
            var item = mapResolution.ValueType;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            Expression result = null;

            try
            {
                var build = typeof(IBinaryDeserializerBuilder)
                    .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate));

                var buildKey = build.MakeGenericMethod(key);
                var buildItem = build.MakeGenericMethod(item);

                var readBlocks = typeof(IBinaryCodec)
                    .GetMethods()
                    .Single(m => m.Name == nameof(IBinaryCodec.ReadBlocks)
                        && m.GetGenericArguments().Length == 2
                    )
                    .MakeGenericMethod(key, item);

                result = Expression.Call(
                    codec,
                    readBlocks,
                    stream,
                    Expression.Constant(
                        buildKey.Invoke(DeserializerBuilder, new object[] { new StringSchema(), cache }),
                        typeof(Func<,>).MakeGenericType(typeof(Stream), key)
                    ),
                    Expression.Constant(
                        buildItem.Invoke(DeserializerBuilder, new object[] { mapSchema.Value, cache }),
                        typeof(Func<,>).MakeGenericType(typeof(Stream), item)
                    )
                );
            }
            catch (TargetInvocationException indirect)
            {
                ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
            }

            var idictionary = typeof(IDictionary<,>)
                .MakeGenericType(key, item);

            var dictionary = typeof(Dictionary<,>)
                .MakeGenericType(key, item);

            var dictionaryConstructor = dictionary
                .GetConstructor(new[] { idictionary });

            var immutableDictionary = typeof(ImmutableDictionary<,>)
                .MakeGenericType(key, item);

            if (target.Equals(immutableDictionary))
            {
                var emptyImmutableDictionary = Expression.Field(null, immutableDictionary.GetField("Empty"));
                var dictionaryResult = Expression.New(dictionaryConstructor, result);
                var addRange = immutableDictionary.GetMethod("AddRange");
                result = Expression.Call(emptyImmutableDictionary, addRange, new[] { dictionaryResult });
            }
            else if (!target.IsAssignableFrom(dictionary))
            {
                throw new UnsupportedTypeException(target, $"A map deserializer cannot be built for type {target.FullName}.");
            }
            else
            {
                result = Expression.ConvertChecked(Expression.New(dictionaryConstructor, result), target);
            }

            var lambda = Expression.Lambda(result, "map deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="NullSchema" />.
    /// </summary>
    public class NullDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a null deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a null value.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="NullSchema" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is NullSchema))
            {
                throw new UnsupportedSchemaException(schema, "A null deserializer can only be built for a null schema.");
            }

            var target = resolution.Type;

            var result = Expression.Default(target);
            var stream = Expression.Parameter(typeof(Stream));

            var lambda = Expression.Lambda(result, "null deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="RecordSchema" /> and attempts to map
    /// it to classes or structs using a constructor to set values.
    /// </summary>
    public class RecordConstructorDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {

        /// <summary>
        /// The deserializer builder to use to build field deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new record deserializer builder case.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build field deserializers.
        /// </param>
        public RecordConstructorDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder;
        }

        /// <summary>
        /// Builds a record deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="RecordSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not a <see cref="RecordResolution" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is RecordResolution recordResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "A record constructor deserializer can only be built for a record resolution.");
            }

            if (!(schema is RecordSchema recordSchema))
            {
                throw new UnsupportedSchemaException(schema, "A record constructor deserializer can only be built for a record schema.");
            }
            
            var schemaFields = recordSchema.Fields.ToList();

            // attempt to find a constructor with arguments fully inclusive of all schema fields by name allowing for extra optional arguments
            ConstructorResolution constructorResolution = recordResolution.Constructors.FirstOrDefault(constructor => IsMatch(constructor, schemaFields));

            if (constructorResolution == null)
            {
                throw new UnsupportedTypeException(recordResolution.Type, "A record constructor deserializer can only be built for a type that has a constructor with all non-optional arguments matching the schema fields name and type.");
            }

            var target = resolution.Type;

            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(target);

            // declare a delegate to construct the object
            Delegate construct = null;

            // bind to this scope:
            Expression result = Expression.Invoke(Expression.Constant((Func<Delegate>)(() => construct)));

            result = Expression.ConvertChecked(result, typeof(Func<,>).MakeGenericType(typeof(Stream), target));

            result = Expression.Block(
                new[] { value },
                Expression.Assign(value, Expression.Invoke(result, stream)),
                value
            );

            var lambda = Expression.Lambda(result, $"{recordSchema.Name} deserializer", new[] { stream });
            var compiled = cache.GetOrAdd((target, schema), lambda.Compile());

            var constructorArguments = new List<(ParameterResolution, ParameterExpression)>();
            // now that an infinite loop won't happen, build the expressions to extract the parameters from the serialized data            
            var extractParameters = recordSchema.Fields.Select(field =>
            {
                // there will be a match or we wouldn't have made it this far.
                var match = constructorResolution.Parameters.Single(f => f.Name.IsMatch(field.Name));

                Expression action = null;
                try
                {
                    var build = typeof(IBinaryDeserializerBuilder)
                        .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate))
                        .MakeGenericMethod(match.Type);

                    action = Expression.Constant(
                        build.Invoke(DeserializerBuilder, new object[] { field.Type, cache }),
                        typeof(Func<,>).MakeGenericType(typeof(Stream), match.Type)
                    );
                }
                catch (TargetInvocationException indirect)
                {
                    ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                }

                // invoke the deserialization
                action = Expression.Invoke(action, stream);
                // create a variable to hold the deserialized data
                var variable = Expression.Parameter(match.Type);
                // assign the deserialized data to the variable
                action = Expression.Assign(variable, action);
                // create a separate list so they can be passed to the constructor in the correct order
                constructorArguments.Add((match, variable));

                return action;
            }).ToList();

            // reorder the parameters to match the order in the constructor and add any default parameters
            var parameters = constructorResolution.Parameters.ToArray();
            Expression[] arguments = new Expression[parameters.Length];
            for (int i = 0; i < parameters.Length; i++)
            {
                var match = constructorArguments.FirstOrDefault(argument => argument.Item1.Name.IsMatch(parameters[i].Name));
                if (match != default)
                {
                    arguments[i] = (match.Item2);
                }
                else
                {
                    arguments[i] = Expression.Constant(parameters[i].Parameter.DefaultValue, parameters[i].Type);
                }
            }

            // add constructing the object to the list of expressions
            extractParameters.Add(Expression.New(constructorResolution.Constructor, arguments));

            result = Expression.Block(constructorArguments.Select(arg => arg.Item2).ToArray(), extractParameters);

            lambda = Expression.Lambda(result, $"{recordSchema.Name} constructor", new[] { stream });
            construct = lambda.Compile();

            return compiled;
        }
               
        /// <summary>
        /// Whether the resolved constructor matches a record schema's fields.
        /// </summary>
        /// <param name="constructor">
        /// The constructor resolution to check for a match.
        /// </param>
        /// <param name="recordFields">
        /// The record schema fields to match against the constructor parameters. 
        /// </param>
        /// <returns></returns>
        protected bool IsMatch(ConstructorResolution constructor, ICollection<RecordField> recordFields)
        {
            if (constructor.Parameters.Count < recordFields.Count)
            {
                return false;
            }

            var matchedFields = 0;
            foreach (var parameter in constructor.Parameters)
            {
                var match = recordFields.SingleOrDefault(field => parameter.Name.IsMatch(field.Name));

                if (match != null)
                {
                    matchedFields++;
                }
                else
                {
                    if (!parameter.Parameter.IsOptional)
                    {
                        return false;
                    }
                }
            }

            return recordFields.Count == matchedFields;
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="RecordSchema" /> and attempts to map
    /// it to classes or structs using property/fields to set values.
    /// </summary>
    public class RecordDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The deserializer builder to use to build field deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new record deserializer builder case.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build field deserializers.
        /// </param>
        public RecordDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder;
        }

        /// <summary>
        /// Builds a record deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="RecordSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not a <see cref="RecordResolution" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is RecordResolution recordResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "A record deserializer can only be built for a record resolution.");
            }

            if (!(schema is RecordSchema recordSchema))
            {
                throw new UnsupportedSchemaException(schema, "A record deserializer can only be built for a record schema.");
            }

            var target = resolution.Type;

            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(target);

            // declare an action that reads/assigns the record fields in order:
            Delegate assign = null;

            // bind to this scope:
            Expression result = Expression.Invoke(Expression.Constant((Func<Delegate>)(() => assign)));

            // coerce Delegate to Action<Stream, TTarget>:
            result = Expression.ConvertChecked(result, typeof(Action<,>).MakeGenericType(typeof(Stream), target));

            // deserialize the record:
            result = Expression.Block(
                new[] { value },
                Expression.Assign(value, Expression.New(target)), // create the thing
                Expression.Invoke(result, stream, value),         // assign its fields
                value                                             // return the thing
            );

            var lambda = Expression.Lambda(result, $"{recordSchema.Name} deserializer", new[] { stream });
            var compiled = cache.GetOrAdd((target, schema), lambda.Compile());

            // now that an infinite cycle won’t happen, build the assign function:
            var assignments = recordSchema.Fields.Select(field =>
            {
                var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));
                var type = match?.Type ?? CreateSurrogateType(field.Type);

                Expression action = null;

                try
                {
                    var build = typeof(IBinaryDeserializerBuilder)
                        .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate))
                        .MakeGenericMethod(type);

                    // https://i.imgur.com/PBLYIc2.gifv
                    action = Expression.Constant(
                        build.Invoke(DeserializerBuilder, new object[] { field.Type, cache }),
                        typeof(Func<,>).MakeGenericType(typeof(Stream), type)
                    );
                }
                catch (TargetInvocationException indirect)
                {
                    ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                }

                // always read to advance the stream:
                action = Expression.Invoke(action, stream);

                if (match != null)
                {
                    // and assign if a field matches:
                    action = Expression.Assign(Expression.PropertyOrField(value, match.Member.Name), action);
                }

                return action;
            }).ToList();

            result = assignments.Count > 0 ? Expression.Block(typeof(void), assignments) : Expression.Empty() as Expression;
            lambda = Expression.Lambda(result, $"{recordSchema.Name} field assigner", new[] { stream, value });
            assign = lambda.Compile();

            return compiled;
        }

        /// <summary>
        /// Creates a type that can be used to deserialize missing record fields.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate a compatible type for.
        /// </param>
        /// <returns>
        /// <see cref="IEnumerable{T}" /> if the schema is an array schema (or a union schema
        /// containing only array/null schemas), <see cref="IDictionary{TKey, TValue}" /> if the
        /// schema is a map schema (or a union schema containing only map/null schemas), and
        /// <see cref="Object" /> otherwise.</returns>
        protected virtual Type CreateSurrogateType(Schema schema)
        {
            var schemas = schema is UnionSchema union
                ? union.Schemas
                : new[] { schema };

            if (schemas.All(s => s is ArraySchema || s is NullSchema))
            {
                var items = schemas.OfType<ArraySchema>()
                    .Select(a => a.Item)
                    .Distinct()
                    .ToList();

                return typeof(IEnumerable<>).MakeGenericType(CreateSurrogateType(
                    items.Count > 1
                        ? new UnionSchema(items)
                        : items.SingleOrDefault()
                ));
            }
            else if (schemas.All(s => s is MapSchema || s is NullSchema))
            {
                var values = schemas.OfType<MapSchema>()
                    .Select(m => m.Value)
                    .Distinct()
                    .ToList();

                return typeof(IDictionary<,>).MakeGenericType(typeof(string), CreateSurrogateType(
                    values.Count > 1
                        ? new UnionSchema(values)
                        : values.SingleOrDefault()
                ));
            }

            return typeof(object);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="StringSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class StringDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new string deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public StringDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a string deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="StringSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no known conversions (<see cref="DateTime" />/<see cref="DateTimeOffset" />,
        /// <see cref="Guid" />, <see cref="TimeSpan"/>, <see cref="Uri" />) can be applied and no
        /// conversion from <see cref="string" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is StringSchema))
            {
                throw new UnsupportedSchemaException(schema, "A string deserializer can only be built for a string schema.");
            }

            var source = typeof(string);
            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readLength = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadInteger));

            Expression result = Expression.ConvertChecked(Expression.Call(codec, readLength, stream), typeof(int));

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Read));

            result = Expression.Call(codec, readValue, stream, result);

            var decodeValue = typeof(Encoding)
                .GetMethod(nameof(Encoding.GetString), new[] { typeof(byte[]) });

            result = Expression.Call(Expression.Constant(Encoding.UTF8), decodeValue, result);

            if (source != target)
            {
                if (target == typeof(DateTime) || target == typeof(DateTime?))
                {
                    var parseDateTime = typeof(DateTime)
                        .GetMethod(nameof(DateTime.Parse), new[]
                        {
                            typeof(string),
                            typeof(IFormatProvider),
                            typeof(DateTimeStyles)
                        });

                    result = Expression.ConvertChecked(
                        Expression.Call(
                            null,
                            parseDateTime,
                            result,
                            Expression.Constant(CultureInfo.InvariantCulture),
                            Expression.Constant(DateTimeStyles.RoundtripKind)
                        ),
                        target
                    );
                }
                else if (target == typeof(DateTimeOffset) || target == typeof(DateTimeOffset?))
                {
                    var parseDateTimeOffset = typeof(DateTimeOffset)
                        .GetMethod(nameof(DateTimeOffset.Parse), new[]
                        {
                            typeof(string),
                            typeof(IFormatProvider),
                            typeof(DateTimeStyles)
                        });

                    result = Expression.ConvertChecked(
                        Expression.Call(
                            null,
                            parseDateTimeOffset,
                            result,
                            Expression.Constant(CultureInfo.InvariantCulture),
                            Expression.Constant(DateTimeStyles.RoundtripKind)
                        ),
                        target
                    );
                }
                else if (target == typeof(Guid) || target == typeof(Guid?))
                {
                    var guidConstructor = typeof(Guid)
                        .GetConstructor(new[] { typeof(string) });

                    result = Expression.New(guidConstructor, result);
                }
                else if (target == typeof(TimeSpan) || target == typeof(TimeSpan?))
                {
                    var parseTimeSpan = typeof(XmlConvert)
                        .GetMethod(nameof(XmlConvert.ToTimeSpan));

                    result = Expression.Call(null, parseTimeSpan, result);
                }
                else if (target == typeof(Uri))
                {
                    var uriConstructor = typeof(Uri)
                        .GetConstructor(new[] { typeof(string) });

                    result = Expression.New(uriConstructor, result);
                }

                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(target, $"A string deserializer cannot be built for type {target.FullName}.", inner);
                }
            }

            var lambda = Expression.Lambda(result, "string deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="MicrosecondTimestampLogicalType" />
    /// or <see cref="MillisecondTimestampLogicalType" /> and attempts to map them to
    /// <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
    /// </summary>
    public class TimestampDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new timestamp deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        public TimestampDeserializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a timestamp deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="LongSchema" /> with logical type
        /// <see cref="MicrosecondTimestampLogicalType" /> or <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type is not <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is LongSchema))
            {
                throw new UnsupportedSchemaException(schema, "A timestamp deserializer can only be built for a long schema.");
            }

            var target = resolution.Type;

            if (!(target == typeof(DateTime) || target == typeof(DateTime?) || target == typeof(DateTimeOffset) || target == typeof(DateTimeOffset?)))
            {
                throw new UnsupportedTypeException(target, $"A timestamp serializer cannot be built for {target.Name}.");
            }

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            Expression epoch = Expression.Constant(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc));
            Expression factor;

            if (schema.LogicalType is MicrosecondTimestampLogicalType)
            {
                factor = Expression.Constant(TimeSpan.TicksPerMillisecond / 1000);
            }
            else if (schema.LogicalType is MillisecondTimestampLogicalType)
            {
                factor = Expression.Constant(TimeSpan.TicksPerMillisecond);
            }
            else
            {
                throw new UnsupportedSchemaException(schema, "A timestamp deserializer can only be built for a schema with a timestamp logical type.");
            }

            var readValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadInteger));

            Expression result = Expression.Call(codec, readValue, stream);

            var addTicks = typeof(DateTime)
                .GetMethod(nameof(DateTime.AddTicks));

            // result = epoch.AddTicks(value * factor);
            result = Expression.ConvertChecked(
                Expression.Call(epoch, addTicks, Expression.Multiply(result, factor)),
                target
            );

            var lambda = Expression.Lambda(result, "timestamp deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="UnionSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class UnionDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The codec that generated deserializers should use for read operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// The deserializer builder to use to build child deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new record deserializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated deserializers should use for read operations.
        /// </param>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build child deserializers.
        /// </param>
        public UnionDeserializerBuilderCase(IBinaryCodec codec, IBinaryDeserializerBuilder deserializerBuilder)
        {
            Codec = codec;
            DeserializerBuilder = deserializerBuilder;
        }

        /// <summary>
        /// Builds a union deserializer for a type-schema pair.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// A delegate cache.
        /// </param>
        /// <returns>
        /// A function that accepts a <see cref="Stream" /> and returns a deserialized object.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="UnionSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type cannot be mapped to each schema in the union.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is UnionSchema unionSchema && unionSchema.Schemas.Count > 0))
            {
                throw new UnsupportedSchemaException(schema, "A union deserializer can only be built for a union schema of one or more schemas.");
            }

            var target = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));

            var readIndex = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.ReadInteger));

            Expression result = Expression.ConvertChecked(Expression.Call(codec, readIndex, stream), typeof(int));

            // create a mapping for each schema in the union:
            var cases = unionSchema.Schemas.Select((child, index) =>
            {
                var selected = SelectType(resolution, child);
                var underlying = Nullable.GetUnderlyingType(selected.Type);

                if (child is NullSchema && selected.Type.IsValueType && underlying == null)
                {
                    throw new UnsupportedTypeException(target, $"A deserializer for a union containing {typeof(NullSchema)} cannot be built for {selected.Type.FullName}.");
                }

                underlying = selected.Type;

                Expression @case = null;

                try
                {
                    var build = typeof(IBinaryDeserializerBuilder)
                        .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate))
                        .MakeGenericMethod(underlying);

                    @case = Expression.Constant(
                        build.Invoke(DeserializerBuilder, new object[] { child, cache }),
                        typeof(Func<,>).MakeGenericType(typeof(Stream), underlying)
                    );
                }
                catch (TargetInvocationException indirect)
                {
                    ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                }

                return Expression.SwitchCase(
                    Expression.ConvertChecked(Expression.Invoke(@case, stream), target),
                    Expression.Constant(index)
                );
            });

            var exceptionConstructor = typeof(OverflowException)
                .GetConstructor(new[] { typeof(string) });

            var exception = Expression.New(exceptionConstructor, Expression.Constant("Union index out of range."));

            // generate a switch on the index:
            result = Expression.Switch(result, Expression.Throw(exception, target), cases.ToArray());

            var lambda = Expression.Lambda(result, "union deserializer", new[] { stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((target, schema), compiled);
        }

        /// <summary>
        /// Customizes type resolutions for the children of a union schema. Can be overriden by
        /// custom cases to support polymorphic mapping.
        /// </summary>
        /// <param name="resolution">
        /// The resolution for the type being mapped to the union schema.
        /// </param>
        /// <param name="schema">
        /// A child of the union schema.
        /// </param>
        /// <returns>
        /// The resolution to build the child deserializer with. The type in the original resolution
        /// must be assignable from the type in the returned resolution.
        /// </returns>
        protected virtual TypeResolution SelectType(TypeResolution resolution, Schema schema)
        {
            return resolution;
        }
    }
}
