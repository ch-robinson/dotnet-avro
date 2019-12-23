using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        Func<Stream, T> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate>? cache = null);

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
    /// Represents the outcome of a deserializer builder case.
    /// </summary>
    public interface IBinaryDeserializerBuildResult
    {
        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        /// <remarks>
        /// The delegate should be a function that accepts a <see cref="Stream" /> and returns a
        /// deserialized object. Since this is not a typed method, the general <see cref="Delegate" />
        /// type is used.
        /// </remarks>
        Delegate? Delegate { get; }

        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Delegate" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        ICollection<Exception> Exceptions { get; }
    }

    /// <summary>
    /// Builds Avro deserializers for specific type-schema combinations. See
    /// <see cref="BinaryDeserializerBuilder" /> for implementation details.
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
        /// A delegate cache. If a delegate is cached for a specific type-schema pair, that same
        /// delegate will be returned for all occurrences of the pair.
        /// </param>
        /// <returns>
        /// A build result.
        /// </returns>
        IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);
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
        /// A resolver to retrieve type information from.
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
        /// A resolver to retrieve type information from. If no resolver is provided, the deserializer
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public BinaryDeserializerBuilder(IBinaryCodec? codec = null, ITypeResolver? resolver = null)
            : this(CreateBinaryDeserializerCaseBuilders(codec ?? new BinaryCodec()), resolver) { }

        /// <summary>
        /// Creates a new deserializer builder.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the deserializer
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public BinaryDeserializerBuilder(IEnumerable<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> caseBuilders, ITypeResolver? resolver = null)
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
        /// The type of object to be deserialized.
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
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the type to the schema.
        /// </exception>
        public virtual Func<Stream, T> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate>? cache = null)
        {
            if (cache == null)
            {
                cache = new ConcurrentDictionary<(Type, Schema), Delegate>();
            }

            var resolution = Resolver.ResolveType(typeof(T));

            if (!cache.TryGetValue((resolution.Type, schema), out var @delegate))
            {
                var exceptions = new List<Exception>();

                foreach (var @case in Cases)
                {
                    var result = @case.BuildDelegate(resolution, schema, cache);

                    if (result.Delegate != null)
                    {
                        @delegate = result.Delegate;
                        break;
                    }

                    exceptions.AddRange(result.Exceptions);
                }

                if (@delegate == null)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"No deserializer builder case matched {resolution.GetType().Name}.", new AggregateException(exceptions));
                }
            }

            return (Func<Stream, T>)@delegate;
        }

        /// <summary>
        /// Builds a binary deserializer.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the type to the schema.
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
    /// A base <see cref="IBinaryDeserializerBuildResult" /> implementation.
    /// </summary>
    public class BinaryDeserializerBuildResult : IBinaryDeserializerBuildResult
    {
        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        /// <remarks>
        /// The delegate should be a function that accepts a <see cref="Stream" /> and returns a
        /// deserialized object. Since this is not a typed method, the general <see cref="Delegate" />
        /// type is used.
        /// </remarks>
        public Delegate? Delegate { get; set; }

        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Delegate" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        public ICollection<Exception> Exceptions { get; set; } = new List<Exception>();
    }

    /// <summary>
    /// A base <see cref="IBinaryDeserializerBuilderCase" /> implementation.
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
        /// A delegate cache. If a delegate is cached for a specific type-schema pair, that same
        /// delegate will be returned for all occurrences of the pair.
        /// </param>
        /// <returns>
        /// A build result.
        /// </returns>
        public abstract IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);

        /// <summary>
        /// Generates a conversion from the intermediate type to the target type.
        /// </summary>
        /// <remarks>
        /// See the remarks for <see cref="Expression.ConvertChecked(Expression, Type)" />.
        /// </remarks>
        protected virtual Expression GenerateConversion(Expression input, Type target)
        {
            if (input.Type == target)
            {
                return input;
            }

            try
            {
                return Expression.ConvertChecked(input, target);
            }
            catch (InvalidOperationException inner)
            {
                throw new UnsupportedTypeException(target, inner: inner);
            }
        }
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
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
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
        /// A successful result if the resolution is an <see cref="ArrayResolution" /> and the
        /// schema is an <see cref="ArraySchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not have an enumerable constructor and is not
        /// assignable from <see cref="List{T}" />.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var target = arrayResolution.Type;
                    var item = arrayResolution.ItemType;

                    var stream = Expression.Parameter(typeof(Stream));

                    Expression expression = null!;

                    try
                    {
                        var build = typeof(IBinaryDeserializerBuilder)
                            .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate))
                            .MakeGenericMethod(item);

                        expression = Codec.ReadList(stream,
                            Expression.Invoke(
                                Expression.Constant(
                                    build.Invoke(DeserializerBuilder, new object[] { arraySchema.Item, cache }),
                                    typeof(Func<,>).MakeGenericType(typeof(Stream), item)),
                                stream));
                    }
                    catch (TargetInvocationException indirect)
                    {
                        ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                    }

                    if (FindEnumerableConstructor(arrayResolution, item) is ConstructorResolution constructorResolution)
                    {
                        expression = Expression.New(constructorResolution.Constructor, new[] { expression });
                    }
                    else
                    {
                        expression = GenerateConversion(expression, target);
                    }

                    var lambda = Expression.Lambda(expression, "array deserializer", new[] { stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((target, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }

        /// <summary>
        /// Attempts to find a constructor that takes a single enumerable parameter.
        /// </summary>
        protected virtual ConstructorResolution? FindEnumerableConstructor(ArrayResolution resolution, Type itemType)
        {
            return resolution.Constructors
                .Where(c => c.Parameters.Count == 1)
                .FirstOrDefault(c => c.Parameters.First().Type.IsAssignableFrom(typeof(IEnumerable<>).MakeGenericType(itemType)));
        }

        /// <summary>
        /// Generates a conversion from the intermediate type to the target type. This override
        /// will convert the intermediate type to an array or list (depending on the target) prior
        /// to applying the base implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type target)
        {
            var convert = target.IsArray
                ? typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.ToArray))
                    .MakeGenericMethod(target.GetElementType())
                : typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.ToList))
                    .MakeGenericMethod(target.GetGenericArguments().Single());

            if (!target.IsAssignableFrom(convert.ReturnType))
            {
                throw new UnsupportedTypeException(target);
            }

            return base.GenerateConversion(Expression.Call(null, convert, input), target);
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
        /// A successful result if the schema is a <see cref="BooleanSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="bool" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is BooleanSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = GenerateConversion(Codec.ReadBoolean(stream), target);
                var lambda = Expression.Lambda(expression, "boolean deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the schema is a <see cref="BytesSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="T:System.Byte[]" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is BytesSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = GenerateConversion(Codec.Read(stream, Expression.ConvertChecked(Codec.ReadInteger(stream), typeof(int))), target);

                var lambda = Expression.Lambda(expression, "bytes deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }

        /// <summary>
        /// Generates a conversion from the source type to the intermediate type. This override
        /// will convert a bytes value to <see cref="Guid" /> prior to applying the base
        /// implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type target)
        {
            if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { input.Type });

                input = Expression.New(guidConstructor, input);
            }

            return base.GenerateConversion(input, target);
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
        /// A successful result if the schema’s logical type is <see cref="DecimalLogicalType" />;
        /// an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BytesSchema" /> or a <see cref="FixedSchema "/>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="decimal" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                Expression expression;

                // figure out the size:
                if (schema is BytesSchema)
                {
                    expression = Expression.ConvertChecked(Codec.ReadInteger(stream), typeof(int));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    expression = Expression.Constant(fixedSchema.Size);
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                // read the bytes:
                expression = Codec.Read(stream, expression);

                // declare some variables for in-place transformation:
                var bytes = Expression.Variable(typeof(byte[]));

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(byte[]) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                expression = Expression.Block(
                    new[] { bytes },

                    // store the bytes in a variable:
                    Expression.Assign(bytes, expression),

                    // BigInteger is little-endian, so reverse before creating:
                    Expression.Call(null, reverse, bytes),

                    // return (decimal)new BigInteger(bytes) / (decimal)Math.Pow(10, scale);
                    Expression.Divide(
                        Expression.ConvertChecked(
                            Expression.New(integerConstructor, bytes),
                            typeof(decimal)),
                        Expression.Constant((decimal)Math.Pow(10, scale)))
                );

                expression = GenerateConversion(expression, target);

                var lambda = Expression.Lambda(expression, "decimal deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the schema is a <see cref="DoubleSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="double" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is DoubleSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = GenerateConversion(Codec.ReadDouble(stream), target);
                var lambda = Expression.Lambda(expression, "double deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the resolution is a <see cref="DurationResolution" /> and the
        /// schema’s logical type is a <see cref="DurationLogicalType" />; an unsuccessful result
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FixedSchema" /> with size 12 and logical
        /// type <see cref="DurationLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="TimeSpan" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema.LogicalType is DurationLogicalType)
            {
                if (resolution is DurationResolution)
                {
                    if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                    {
                        throw new UnsupportedSchemaException(schema);
                    }

                    Func<Stream, long> read = input =>
                    {
                        var buffer = new byte[4];
                        var bytes = input.Read(buffer, 0, buffer.Length);

                        if (!BitConverter.IsLittleEndian)
                        {
                            Array.Reverse(buffer);
                        }

                        return BitConverter.ToUInt32(buffer, 0);
                    };

                    var target = resolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));

                    var expression = GenerateConversion(Expression.Block(
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
                        )
                    ), target);

                    var lambda = Expression.Lambda(expression, $"duration deserializer", new[] { stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((target, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the resolution is an <see cref="EnumResolution" /> and the
        /// schema is an <see cref="EnumSchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the the type does not contain a matching symbol for each symbol in the
        /// schema.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var target = resolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));

                    Expression expression = Expression.ConvertChecked(Codec.ReadInteger(stream), typeof(int));

                    // find a match for each enum in the schema:
                    var cases = enumSchema.Symbols.Select((name, index) =>
                    {
                        var match = enumResolution.Symbols.SingleOrDefault(s => s.Name.IsMatch(name));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(target, $"{target.Name} has no value that matches {name}.");
                        }

                        return Expression.SwitchCase(
                            GenerateConversion(Expression.Constant(match.Value), target),
                            Expression.Constant(index)
                        );
                    });

                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum index out of range."));

                    // generate a switch on the index:
                    expression = Expression.Switch(expression, Expression.Throw(exception, target), cases.ToArray());

                    var lambda = Expression.Lambda(expression, $"{enumSchema.Name} deserializer", new[] { stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((target, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the schema is a <see cref="FixedSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="T:System.Byte[]" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is FixedSchema fixedSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = GenerateConversion(Codec.Read(stream, Expression.Constant(fixedSchema.Size)), target);
                var lambda = Expression.Lambda(expression, $"{fixedSchema.Name} deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }

        /// <summary>
        /// Generates a conversion from the source type to the intermediate type. This override
        /// will convert a bytes value to <see cref="Guid" /> prior to applying the base
        /// implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type target)
        {
            if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { input.Type });

                input = Expression.New(guidConstructor, input);
            }

            return base.GenerateConversion(input, target);
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
        /// A successful result if the schema is a <see cref="FloatSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="float" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is FloatSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = GenerateConversion(Codec.ReadSingle(stream), target);
                var lambda = Expression.Lambda(expression, "float deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the schema is an <see cref="IntSchema" /> or a <see cref="LongSchema" />;
        /// an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="long" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is IntSchema || schema is LongSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = GenerateConversion(Codec.ReadInteger(stream), target);
                var lambda = Expression.Lambda(expression, "integer deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
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
        /// A successful result if the resolution is a <see cref="MapResolution" /> and the schema
        /// is a <see cref="MapSchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type is not assignable from <see cref="Dictionary{TKey, TValue}" />.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is MapSchema mapSchema)
            {
                if (resolution is MapResolution mapResolution)
                {
                    var target = mapResolution.Type;
                    var key = mapResolution.KeyType;
                    var item = mapResolution.ValueType;

                    var stream = Expression.Parameter(typeof(Stream));

                    Expression expression = null!;

                    try
                    {
                        var build = typeof(IBinaryDeserializerBuilder)
                            .GetMethod(nameof(IBinaryDeserializerBuilder.BuildDelegate));

                        var buildKey = build.MakeGenericMethod(key);
                        var buildItem = build.MakeGenericMethod(item);

                        expression = Codec.ReadDictionary(stream,
                            Expression.Invoke(
                                Expression.Constant(
                                    buildKey.Invoke(DeserializerBuilder, new object[] { new StringSchema(), cache }),
                                    typeof(Func<,>).MakeGenericType(typeof(Stream), key)),
                                stream),
                            Expression.Invoke(
                                Expression.Constant(
                                    buildItem.Invoke(DeserializerBuilder, new object[] { mapSchema.Value, cache }),
                                    typeof(Func<,>).MakeGenericType(typeof(Stream), item)),
                                stream));
                    }
                    catch (TargetInvocationException indirect)
                    {
                        ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                    }

                    expression = GenerateConversion(expression, target);

                    var lambda = Expression.Lambda(expression, "map deserializer", new[] { stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((target, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the schema is a <see cref="NullSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is NullSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = Expression.Default(target);
                var lambda = Expression.Lambda(expression, "null deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
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
        /// A successful result if the resolution is a <see cref="RecordResolution" />, the
        /// schema is a <see cref="RecordSchema" />, and the resolved type has a constructor with
        /// a matching parameter for each field on the schema; an unsuccessful result otherwise.
        /// </returns>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    var schemaFields = recordSchema.Fields.ToList();

                    if (recordResolution.Constructors.FirstOrDefault(constructor => IsMatch(constructor, schemaFields)) is ConstructorResolution constructorResolution)
                    {
                        var target = resolution.Type;

                        var stream = Expression.Parameter(typeof(Stream));
                        var value = Expression.Parameter(target);

                        // declare a delegate to construct the object
                        Delegate construct = null!;

                        // bind to this scope:
                        Expression expression = Expression.Invoke(Expression.Constant((Func<Delegate>)(() => construct)));

                        expression = GenerateConversion(expression, typeof(Func<,>).MakeGenericType(typeof(Stream), target));

                        expression = Expression.Block(
                            new[] { value },
                            Expression.Assign(value, Expression.Invoke(expression, stream)),
                            value
                        );

                        var lambda = Expression.Lambda(expression, $"{recordSchema.Name} deserializer", new[] { stream });
                        var compiled = lambda.Compile();

                        if (!cache.TryAdd((target, schema), compiled))
                        {
                            throw new InvalidOperationException();
                        }

                        result.Delegate = compiled;

                        // now that an infinite loop won't happen, build the expressions to extract the parameters from the serialized data
                        var constructorArguments = new List<(ParameterResolution, ParameterExpression)>();
                        var extractParameters = recordSchema.Fields.Select(field =>
                        {
                            // there will be a match or we wouldn't have made it this far.
                            var match = constructorResolution.Parameters.Single(f => f.Name.IsMatch(field.Name));

                            Expression action = null!;
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

                        expression = Expression.Block(constructorArguments.Select(arg => arg.Item2).ToArray(), extractParameters);

                        lambda = Expression.Lambda(expression, $"{recordSchema.Name} constructor", new[] { stream });
                        construct = lambda.Compile();
                    }
                    else
                    {
                        result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                    }
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
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
        /// A successful result if the resolution is a <see cref="RecordResolution" /> and the
        /// schema is a <see cref="RecordSchema" />; an unsuccessful result otherwise.
        /// </returns>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    var target = resolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));
                    var value = Expression.Parameter(target);

                    // declare an action that reads/assigns the record fields in order:
                    Delegate assign = null!;

                    // bind to this scope:
                    Expression expression = Expression.Invoke(Expression.Constant((Func<Delegate>)(() => assign)));

                    // coerce Delegate to Action<Stream, TTarget>:
                    expression = GenerateConversion(expression, typeof(Action<,>).MakeGenericType(typeof(Stream), target));

                    // deserialize the record:
                    expression = Expression.Block(
                        new[] { value },
                        Expression.Assign(value, Expression.New(target)), // create the thing
                        Expression.Invoke(expression, stream, value),     // assign its fields
                        value                                             // return the thing
                    );

                    var lambda = Expression.Lambda(expression, $"{recordSchema.Name} deserializer", new[] { stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((target, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;

                    // now that an infinite cycle won’t happen, build the assign function:
                    var assignments = recordSchema.Fields.Select(field =>
                    {
                        var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));
                        var type = match?.Type ?? CreateSurrogateType(field.Type);

                        Expression action = null!;

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

                    expression = assignments.Count > 0 ? Expression.Block(typeof(void), assignments) : Expression.Empty() as Expression;
                    lambda = Expression.Lambda(expression, $"{recordSchema.Name} field assigner", new[] { stream, value });
                    assign = lambda.Compile();
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type, "A record deserializer can only be built for a record resolution."));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
        /// A successful result if the schema is a <see cref="StringSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="string" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is StringSchema)
            {
                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                var expression = Codec.Read(stream, Expression.ConvertChecked(Codec.ReadInteger(stream), typeof(int)));

                var decodeValue = typeof(Encoding)
                    .GetMethod(nameof(Encoding.GetString), new[] { typeof(byte[]) });

                expression = GenerateConversion(Expression.Call(Expression.Constant(Encoding.UTF8), decodeValue, expression), target);

                var lambda = Expression.Lambda(expression, "string deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }

        /// <summary>
        /// Generates a conversion from the source type to the intermediate type. This override
        /// will convert a string value to <see cref="DateTime" />, <see cref="DateTimeOffset" />,
        /// <see cref="Guid" />, <see cref="TimeSpan" />, or <see cref="Uri" /> prior to applying
        /// the base implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type target)
        {
            if (target == typeof(DateTime) || target == typeof(DateTime?))
            {
                var parseDateTime = typeof(DateTime)
                    .GetMethod(nameof(DateTime.Parse), new[]
                    {
                        input.Type,
                        typeof(IFormatProvider),
                        typeof(DateTimeStyles)
                    });

                input = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateTime,
                        input,
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
                        input.Type,
                        typeof(IFormatProvider),
                        typeof(DateTimeStyles)
                    });

                input = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateTimeOffset,
                        input,
                        Expression.Constant(CultureInfo.InvariantCulture),
                        Expression.Constant(DateTimeStyles.RoundtripKind)
                    ),
                    target
                );
            }
            else if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { input.Type });

                input = Expression.New(guidConstructor, input);
            }
            else if (target == typeof(TimeSpan) || target == typeof(TimeSpan?))
            {
                var parseTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToTimeSpan));

                input = Expression.Call(null, parseTimeSpan, input);
            }
            else if (target == typeof(Uri))
            {
                var uriConstructor = typeof(Uri)
                    .GetConstructor(new[] { input.Type });

                input = Expression.New(uriConstructor, input);
            }

            return base.GenerateConversion(input, target);
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
        /// A successful result if the resolution is a <see cref="TimestampResolution" /> and the
        /// schema’s logical type is a <see cref="TimestampLogicalType" />; an unsuccessful result
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="LongSchema" /> with logical type
        /// <see cref="MicrosecondTimestampLogicalType" /> or <see cref="MillisecondTimestampLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="DateTime" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema.LogicalType is TimestampLogicalType)
            {
                if (resolution is TimestampResolution)
                {
                    if (!(schema is LongSchema))
                    {
                        throw new UnsupportedSchemaException(schema);
                    }

                    var target = resolution.Type;

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
                        throw new UnsupportedSchemaException(schema);
                    }

                    Expression expression = Codec.ReadInteger(stream);

                    var addTicks = typeof(DateTime)
                        .GetMethod(nameof(DateTime.AddTicks));

                    // result = epoch.AddTicks(value * factor);
                    expression = GenerateConversion(
                        Expression.Call(epoch, addTicks, Expression.Multiply(expression, factor)),
                        target
                    );

                    var lambda = Expression.Lambda(expression, "timestamp deserializer", new[] { stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((target, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;
                }
                else
                {
                    result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
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
        /// A successful result if the schema is a <see cref="UnionSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the union schema is empty.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type cannot be mapped to each schema in the union.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var target = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));

                Expression expression = Expression.ConvertChecked(Codec.ReadInteger(stream), typeof(int));

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

                    Expression @case = null!;

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
                        GenerateConversion(Expression.Invoke(@case, stream), target),
                        Expression.Constant(index)
                    );
                });

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                var exception = Expression.New(exceptionConstructor, Expression.Constant("Union index out of range."));

                // generate a switch on the index:
                expression = Expression.Switch(expression, Expression.Throw(exception, target), cases.ToArray());

                var lambda = Expression.Lambda(expression, "union deserializer", new[] { stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((target, schema), compiled))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = compiled;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
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
