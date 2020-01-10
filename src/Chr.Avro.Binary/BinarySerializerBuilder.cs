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
    /// Builds Avro serializers for .NET types.
    /// </summary>
    public interface IBinarySerializerBuilder
    {
        /// <summary>
        /// Builds a delegate that writes a serialized object to a stream.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// An optional delegate cache. The cache can be used to provide custom implementations for
        /// particular type-schema pairs, and it will also be populated as the delegate is built.
        /// </param>
        /// <returns>
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        Action<T, Stream> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate>? cache = null);

        /// <summary>
        /// Builds a binary serializer.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        IBinarySerializer<T> BuildSerializer<T>(Schema schema);
    }

    /// <summary>
    /// Represents the outcome of a serializer builder case.
    /// </summary>
    public interface IBinarySerializerBuildResult
    {
        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        /// <remarks>
        /// The delegate should be an action that accepts an object and a <see cref="Stream" />.
        /// Since this is not a typed method, the general <see cref="Delegate" /> type is used.
        /// </remarks>
        Delegate? Delegate { get; }

        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Delegate" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        ICollection<Exception> Exceptions { get; }
    }

    /// <summary>
    /// Builds Avro serializers for specific type-schema combinations. Used by
    /// <see cref="BinarySerializerBuilder" /> to break apart serializer building logic.
    /// </summary>
    public interface IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a serializer for a type-schema pair.
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
        /// A build result.
        /// </returns>
        IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);
    }

    /// <summary>
    /// A serializer builder configured with a reasonable set of default cases.
    /// </summary>
    public class BinarySerializerBuilder : IBinarySerializerBuilder
    {
        /// <summary>
        /// A list of cases that the build methods will attempt to apply. If the first case does
        /// not match, the next case will be tested, and so on.
        /// </summary>
        public IEnumerable<IBinarySerializerBuilderCase> Cases { get; }

        /// <summary>
        /// A resolver to retrieve type information from.
        /// </summary>
        public ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates a new serializer builder.
        /// </summary>
        /// <param name="codec">
        /// A codec implementation that generated serializers will use for write operations. If no
        /// codec is provided, <see cref="BinaryCodec" /> will be used.
        /// </param>
        /// <param name="resolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the serializer
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public BinarySerializerBuilder(IBinaryCodec? codec = null, ITypeResolver? resolver = null)
            : this(CreateBinarySerializerCaseBuilders(codec ?? new BinaryCodec()), resolver) { }

        /// <summary>
        /// Creates a new serializer builder.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the serializer
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public BinarySerializerBuilder(IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> caseBuilders, ITypeResolver? resolver = null)
        {
            var cases = new List<IBinarySerializerBuilderCase>();

            Cases = cases;
            Resolver = resolver ?? new DataContractResolver();

            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Builds a delegate that writes a serialized object to a stream.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="cache">
        /// An optional delegate cache. The cache can be used to provide custom implementations for
        /// particular type-schema pairs, and it will also be populated as the delegate is built.
        /// </param>
        /// <returns>
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the type to the schema.
        /// </exception>
        public Action<T, Stream> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate>? cache = null)
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
                    throw new UnsupportedTypeException(resolution.Type, $"No serializer builder case matched {resolution.GetType().Name}.", new AggregateException(exceptions));
                }
            }

            return (Action<T, Stream>)@delegate;
        }

        /// <summary>
        /// Builds a binary serializer.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the type to the schema.
        /// </exception>
        public IBinarySerializer<T> BuildSerializer<T>(Schema schema)
        {
            return new BinarySerializer<T>(BuildDelegate<T>(schema));
        }

        /// <summary>
        /// Creates a default list of case builders.
        /// </summary>
        /// <param name="codec">
        /// A codec implementation that generated serializers will use for write operations.
        /// </param>
        public static IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> CreateBinarySerializerCaseBuilders(IBinaryCodec codec)
        {
            return new Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>[]
            {
                // logical types:
                builder => new DecimalSerializerBuilderCase(codec),
                builder => new DurationSerializerBuilderCase(codec),
                builder => new TimestampSerializerBuilderCase(codec),

                // primitives:
                builder => new BooleanSerializerBuilderCase(codec),
                builder => new BytesSerializerBuilderCase(codec),
                builder => new DoubleSerializerBuilderCase(codec),
                builder => new FixedSerializerBuilderCase(codec),
                builder => new FloatSerializerBuilderCase(codec),
                builder => new IntegerSerializerBuilderCase(codec),
                builder => new NullSerializerBuilderCase(),
                builder => new StringSerializerBuilderCase(codec),

                // collections:
                builder => new ArraySerializerBuilderCase(codec, builder),
                builder => new MapSerializerBuilderCase(codec, builder),

                // enums:
                builder => new EnumSerializerBuilderCase(codec),

                // records:
                builder => new RecordSerializerBuilderCase(builder),

                // unions:
                builder => new UnionSerializerBuilderCase(codec, builder)
            };
        }
    }

    /// <summary>
    /// A base <see cref="IBinarySerializerBuildResult" /> implementation.
    /// </summary>
    public class BinarySerializerBuildResult : IBinarySerializerBuildResult
    {
        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        /// <remarks>
        /// The delegate should be an action that accepts an object and a <see cref="Stream" />.
        /// Since this is not a typed method, the general <see cref="Delegate" /> type is used.
        /// </remarks>
        public Delegate? Delegate { get; set; }

        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Delegate" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        public ICollection<Exception> Exceptions { get; set; } = new List<Exception>();
    }

    /// <summary>
    /// A base <see cref="IBinarySerializerBuilderCase" /> implementation.
    /// </summary>
    public abstract class BinarySerializerBuilderCase : IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a serializer for a type-schema pair.
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
        public abstract IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);

        /// <summary>
        /// Generates a conversion from the source type to the intermediate type.
        /// </summary>
        /// <remarks>
        /// See the remarks for <see cref="Expression.ConvertChecked(Expression, Type)" />.
        /// </remarks>
        protected virtual Expression GenerateConversion(Expression input, Type intermediate)
        {
            if (input.Type == intermediate)
            {
                return input;
            }

            try
            {
                return Expression.ConvertChecked(input, intermediate);
            }
            catch (InvalidOperationException inner)
            {
                throw new UnsupportedTypeException(intermediate, inner: inner);
            }
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="ArraySchema" /> and attempts to map it to
    /// enumerable types.
    /// </summary>
    public class ArraySerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// The serializer builder to use to build item serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new array serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build item serializers.
        /// </param>
        public ArraySerializerBuilderCase(IBinaryCodec codec, IBinarySerializerBuilder serializerBuilder)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Builds an array serializer for a type-schema pair.
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
        /// Thrown when the resolved type does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var source = arrayResolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));
                    var value = Expression.Parameter(source);

                    Expression expression = null!;

                    try
                    {
                        var build = typeof(IBinarySerializerBuilder)
                            .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
                            .MakeGenericMethod(arrayResolution.ItemType);

                        var itemVariable = Expression.Variable(arrayResolution.ItemType);

                        expression = Codec.WriteArray(
                            value,
                            itemVariable,
                            Expression.Invoke(
                                Expression.Constant(
                                    build.Invoke(SerializerBuilder, new object[] { arraySchema.Item, cache }),
                                    typeof(Action<,>).MakeGenericType(arrayResolution.ItemType, typeof(Stream))),
                                itemVariable,
                                stream
                            ),
                            stream);
                    }
                    catch (TargetInvocationException indirect)
                    {
                        ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                    }

                    var lambda = Expression.Lambda(expression, "array serializer", new[] { value, stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="BooleanSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class BooleanSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new boolean serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public BooleanSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a boolean serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="bool" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is BooleanSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(bool));
                var lambda = Expression.Lambda(Codec.WriteBoolean(expression, stream), "boolean serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="BytesSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class BytesSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new variable-length bytes serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public BytesSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a variable-length bytes serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is BytesSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(byte[]));
                var bytes = Expression.Variable(expression.Type);

                expression = Expression.Block(
                    new[] { bytes },
                    Expression.Assign(bytes, expression),
                    Codec.WriteInteger(Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), stream),
                    Codec.Write(bytes, stream)
                );

                var lambda = Expression.Lambda(expression, "bytes serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
        /// will convert a <see cref="Guid" /> value to a byte array prior to applying the base
        /// implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type intermediate)
        {
            if (input.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                input = Expression.Call(input, convertGuid);
            }

            return base.GenerateConversion(input, intermediate);
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="DecimalLogicalType" /> and attempts to
    /// map it to any provided type.
    /// </summary>
    public class DecimalSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new decimal serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public DecimalSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a decimal serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="decimal" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(decimal));

                // buffer:
                var bytes = Expression.Variable(typeof(byte[]));

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(decimal) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                var toByteArray = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

                expression = Expression.Block(
                    // bytes = new BigInteger(result * (decimal)Math.Pow(10, scale)).ToByteArray();
                    Expression.Assign(bytes,
                        Expression.Call(
                            Expression.New(integerConstructor,
                                Expression.Multiply(
                                    expression,
                                    Expression.Constant((decimal)Math.Pow(10, scale)))),
                            toByteArray)),

                    // BigInteger is little-endian, so reverse:
                    Expression.Call(null, reverse, bytes),

                    // return byte array:
                    bytes
                );

                // figure out how to write:
                if (schema is BytesSchema)
                {
                    expression = Expression.Block(
                        new[] { bytes },
                        expression,
                        Codec.WriteInteger(Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), stream),
                        Codec.Write(bytes, stream)
                    );
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    expression = Expression.Block(
                        new[] { bytes },
                        expression,
                        Expression.IfThen(
                            Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema.Name} (size {fixedSchema.Size}) and decimal with precision {precision} and scale {scale}.")))
                        ),
                        Codec.Write(bytes, stream)
                    );
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var lambda = Expression.Lambda(expression, "decimal serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="DoubleSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class DoubleSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new double serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public DoubleSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a double serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="double" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is DoubleSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(double));
                var lambda = Expression.Lambda(Codec.WriteFloat(expression, stream), "double serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="DurationLogicalType" /> and attempts to
    /// map it to <see cref="TimeSpan" />.
    /// </summary>
    public class DurationSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new duration serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public DurationSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a duration serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="TimeSpan" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema.LogicalType is DurationLogicalType)
            {
                if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var source = resolution.Type;

                if (!(source == typeof(TimeSpan)))
                {
                    throw new UnsupportedTypeException(resolution.Type);
                }

                void write(uint value, Stream stream)
                {
                    var bytes = BitConverter.GetBytes(value);

                    if (!BitConverter.IsLittleEndian)
                    {
                        Array.Reverse(bytes);
                    }

                    stream.Write(bytes, 0, bytes.Length);
                }

                Action<TimeSpan, Stream> serialize = (value, stream) =>
                {
                    var months = 0U;
                    var days = Convert.ToUInt32(value.TotalDays);
                    var milliseconds = Convert.ToUInt32((ulong)value.TotalMilliseconds - (days * 86400000UL));

                    write(months, stream);
                    write(days, stream);
                    write(milliseconds, stream);
                };

                if (!cache.TryAdd((source, schema), serialize))
                {
                    throw new InvalidOperationException();
                }

                result.Delegate = serialize;
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="EnumSchema" /> and attempts to map it to
    /// enum types.
    /// </summary>
    public class EnumSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new enum serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public EnumSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds an enum serializer for a type-schema pair.
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
        /// Thrown when the schema does not contain a matching symbol for each symbol in the type.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var source = resolution.Type;
                    var symbols = enumSchema.Symbols.ToList();

                    var stream = Expression.Parameter(typeof(Stream));
                    var value = Expression.Parameter(source);

                    // find a match for each enum in the type:
                    var cases = enumResolution.Symbols.Select(symbol =>
                    {
                        var index = symbols.FindIndex(s => symbol.Name.IsMatch(s));

                        if (index < 0)
                        {
                            throw new UnsupportedTypeException(source, $"{source.Name} has a symbol ({symbol.Name}) that cannot be serialized.");
                        }

                        if (symbols.FindLastIndex(s => symbol.Name.IsMatch(s)) != index)
                        {
                            throw new UnsupportedTypeException(source, $"{source.Name} has an ambiguous symbol ({symbol.Name}).");
                        }

                        return Expression.SwitchCase(Codec.WriteInteger(Expression.Constant((long)index), stream), Expression.Constant(symbol.Value));
                    });

                    var expression = Expression.Switch(value, cases.ToArray());
                    var lambda = Expression.Lambda(expression, $"{enumSchema.Name} serializer", new[] { value, stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="FixedSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class FixedSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new fixed-length bytes serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public FixedSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a fixed-length bytes serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is FixedSchema fixedSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(byte[]));

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                expression = Expression.Block(
                    Expression.IfThen(
                        Expression.NotEqual(Expression.ArrayLength(expression), Expression.Constant(fixedSchema.Size)),
                        Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Only arrays of size {fixedSchema.Size} can be serialized to {fixedSchema.Name}.")))
                    ),
                    Codec.Write(expression, stream)
                );

                var lambda = Expression.Lambda(expression, $"{fixedSchema.Name} serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
        /// will convert a <see cref="Guid" /> value to a byte array prior to applying the base
        /// implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type intermediate)
        {
            if (input.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                input = Expression.Call(input, convertGuid);
            }

            return base.GenerateConversion(input, intermediate);
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="FloatSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class FloatSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new float serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public FloatSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a float serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="float" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is FloatSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(float));
                var lambda = Expression.Lambda(Codec.WriteFloat(expression, stream), "float serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="IntSchema" /> or <see cref="LongSchema" />
    /// and attempts to map them to any provided type.
    /// </summary>
    public class IntegerSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new integer serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public IntegerSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds an integer serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="long" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is IntSchema || schema is LongSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(long));
                var lambda = Expression.Lambda(Codec.WriteInteger(expression, stream), "integer serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="MapSchema" /> and attempts to map it to
    /// dictionary types.
    /// </summary>
    public class MapSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// The serializer builder to use to build key and value serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new map serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build key and value serializers.
        /// </param>
        public MapSerializerBuilderCase(IBinaryCodec codec, IBinarySerializerBuilder serializerBuilder)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Builds a map serializer for a type-schema pair.
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
        /// Thrown when the resolved type does not implement <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is MapSchema mapSchema)
            {
                if (resolution is MapResolution mapResolution)
                {
                    var source = mapResolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));
                    var value = Expression.Parameter(source);

                    Expression expression = value;

                    try
                    {
                        var build = typeof(IBinarySerializerBuilder)
                            .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate));

                        var buildKey = build.MakeGenericMethod(mapResolution.KeyType);
                        var buildValue = build.MakeGenericMethod(mapResolution.ValueType);

                        var keyVariable = Expression.Variable(mapResolution.KeyType);
                        var valueVariable = Expression.Variable(mapResolution.ValueType);

                        expression = Codec.WriteMap(
                            value,
                            keyVariable,
                            valueVariable,
                            Expression.Invoke(
                                Expression.Constant(
                                    buildKey.Invoke(SerializerBuilder, new object[] { new StringSchema(), cache }),
                                    typeof(Action<,>).MakeGenericType(mapResolution.KeyType, typeof(Stream))),
                                keyVariable,
                                stream),
                            Expression.Invoke(
                                Expression.Constant(
                                    buildValue.Invoke(SerializerBuilder, new object[] { mapSchema.Value, cache }),
                                    typeof(Action<,>).MakeGenericType(mapResolution.ValueType, typeof(Stream))),
                                valueVariable,
                                stream),
                            stream);
                    }
                    catch (TargetInvocationException indirect)
                    {
                        ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
                    }

                    var lambda = Expression.Lambda(expression, "map serializer", new[] { value, stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="NullSchema" />.
    /// </summary>
    public class NullSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a null serializer for a type-schema pair.
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
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is NullSchema)
            {
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var lambda = Expression.Lambda(Expression.Empty(), "null serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="RecordSchema" /> and attempts to map
    /// it to classes or structs.
    /// </summary>
    public class RecordSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The serializer builder to use to build field serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new record serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build field serializers.
        /// </param>
        public RecordSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Builds a record serializer for a type-schema pair.
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
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not have a matching member for each field on the
        /// schema.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    var source = resolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));
                    var value = Expression.Parameter(source);

                    // declare an action that writes the record fields in order:
                    Delegate write = null!;

                    // bind to this scope:
                    Expression expression = Expression.Invoke(Expression.Constant((Func<Delegate>)(() => write)));

                    // coerce Delegate to Action<TSource, Stream>:
                    expression = Expression.ConvertChecked(expression, typeof(Action<,>).MakeGenericType(source, typeof(Stream)));

                    // serialize the record:
                    expression = Expression.Invoke(expression, value, stream);

                    var lambda = Expression.Lambda(expression, $"{recordSchema.Name} serializer", new[] { value, stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((source, schema), compiled))
                    {
                        throw new InvalidOperationException();
                    }

                    result.Delegate = compiled;

                    // now that an infinite cycle won’t happen, build the write function:
                    var writes = recordSchema.Fields.Select(field =>
                    {
                        var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(source, $"{source.FullName} does not have a field or property that matches the {field.Name} field on {recordSchema.Name}.");
                        }

                        var type = match.Type;

                        Expression action = null!;

                        try
                        {
                            var build = typeof(IBinarySerializerBuilder)
                                .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
                                .MakeGenericMethod(type);

                            // https://i.imgur.com/kZW9iiW.gif
                            action = Expression.Constant(
                                build.Invoke(SerializerBuilder, new object[] { field.Type, cache }),
                                typeof(Action<,>).MakeGenericType(type, typeof(Stream))
                            );
                        }
                        catch (TargetInvocationException exception)
                        {
                            ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
                        }

                        Expression getter = Expression.PropertyOrField(value, match.Member.Name);

                        // do the write:
                        action = Expression.Invoke(action, getter, stream);

                        return action;
                    }).ToList();

                    expression = writes.Count > 0 ? Expression.Block(typeof(void), writes) : Expression.Empty() as Expression;
                    lambda = Expression.Lambda(expression, $"{recordSchema.Name} field writer", new[] { value, stream });
                    write = lambda.Compile();
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
    /// A serializer builder case that matches <see cref="StringSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class StringSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new string serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public StringSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a string serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="string" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is StringSchema)
            {
                var source = resolution.Type;
                var target = typeof(string);

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                var expression = GenerateConversion(value, typeof(string));

                var convertString = typeof(Encoding)
                    .GetMethod(nameof(Encoding.GetBytes), new[] { typeof(string) });

                expression = Expression.Call(Expression.Constant(Encoding.UTF8), convertString, expression);

                var bytes = Expression.Variable(expression.Type);

                expression = Expression.Block(
                    new[] { bytes },
                    Expression.Assign(bytes, expression),
                    Codec.WriteInteger(Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), stream),
                    Codec.Write(bytes, stream)
                );

                var lambda = Expression.Lambda(expression, "string serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
        /// will convert a <see cref="DateTime" />, <see cref="DateTimeOffset" />, <see cref="Guid" />,
        /// <see cref="TimeSpan" />, or <see cref="Uri" /> value to a string prior to applying the
        /// base implementation.
        /// </summary>
        protected override Expression GenerateConversion(Expression input, Type intermediate)
        {
            if (input.Type == typeof(DateTime))
            {
                var convertDateTime = typeof(DateTime)
                    .GetMethod(nameof(DateTime.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                input = Expression.Call(
                    input,
                    convertDateTime,
                    Expression.Constant("O"),
                    Expression.Constant(CultureInfo.InvariantCulture)
                );
            }
            else if (input.Type == typeof(DateTimeOffset))
            {
                var convertDateTimeOffset = typeof(DateTimeOffset)
                    .GetMethod(nameof(DateTimeOffset.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                input = Expression.Call(
                    input,
                    convertDateTimeOffset,
                    Expression.Constant("O"),
                    Expression.Constant(CultureInfo.InvariantCulture)
                );
            }
            else if (input.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToString), Type.EmptyTypes);

                input = Expression.Call(input, convertGuid);
            }
            else if (input.Type == typeof(TimeSpan))
            {
                var convertTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToString), new[] { typeof(TimeSpan) });

                input = Expression.Call(null, convertTimeSpan, input);
            }
            else if (input.Type == typeof(Uri))
            {
                var convertUri = typeof(Uri)
                    .GetMethod(nameof(Uri.ToString));

                input = Expression.Call(input, convertUri);
            }

            return base.GenerateConversion(input, intermediate);
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="MicrosecondTimestampLogicalType" />
    /// or <see cref="MillisecondTimestampLogicalType" /> and attempts to map them to
    /// <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
    /// </summary>
    public class TimestampSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// Creates a new timestamp serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        public TimestampSerializerBuilderCase(IBinaryCodec codec)
        {
            Codec = codec ?? throw new ArgumentNullException(nameof(codec), "Binary codec cannot be null.");
        }

        /// <summary>
        /// Builds a timestamp serializer for a type-schema pair.
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
        /// Thrown when the resolved type cannot be converted to <see cref="DateTimeOffset" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema.LogicalType is TimestampLogicalType)
            {
                if (resolution is TimestampResolution)
                {
                    if (!(schema is LongSchema))
                    {
                        throw new UnsupportedSchemaException(schema);
                    }

                    var source = resolution.Type;

                    var stream = Expression.Parameter(typeof(Stream));
                    var value = Expression.Parameter(source);

                    var expression = GenerateConversion(value, typeof(DateTimeOffset));

                    Expression epoch = Expression.Constant(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks);
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

                    var utcTicks = typeof(DateTimeOffset)
                        .GetProperty(nameof(DateTimeOffset.UtcTicks));

                    // result = (value.UtcTicks - epoch) / factor;
                    expression = Codec.WriteInteger(Expression.Divide(Expression.Subtract(Expression.Property(expression, utcTicks), epoch), factor), stream);

                    var lambda = Expression.Lambda(expression, "timestamp serializer", new[] { value, stream });
                    var compiled = lambda.Compile();

                    if (!cache.TryAdd((source, schema), compiled))
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
    /// A serializer builder case that matches <see cref="UnionSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class UnionSerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The codec that generated serializers should use for write operations.
        /// </summary>
        public IBinaryCodec Codec { get; }

        /// <summary>
        /// The serializer builder to use to build child serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new union serializer builder case.
        /// </summary>
        /// <param name="codec">
        /// The codec that generated serializers should use for write operations.
        /// </param>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build child serializers.
        /// </param>
        public UnionSerializerBuilderCase(IBinaryCodec codec, IBinarySerializerBuilder serializerBuilder)
        {
            Codec = codec;
            SerializerBuilder = serializerBuilder;
        }

        /// <summary>
        /// Builds a union serializer for a type-schema pair.
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
        /// Thrown when the type cannot be mapped to at least one schema in the union.
        /// </exception>
        public override IBinarySerializerBuildResult BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var schemas = unionSchema.Schemas.ToList();
                var candidates = schemas.Where(s => !(s is NullSchema)).ToList();
                var @null = schemas.Find(s => s is NullSchema);
                var source = resolution.Type;

                var stream = Expression.Parameter(typeof(Stream));
                var value = Expression.Parameter(source);

                Expression writeIndex(Schema child) => Codec.WriteInteger(
                    Expression.Constant((long)schemas.IndexOf(child)),
                    stream);

                Expression expression = null!;

                // if there are non-null schemas, select the first matching one for each possible type:
                if (candidates.Count > 0)
                {
                    var cases = new Dictionary<Type, Expression>();
                    var exceptions = new List<Exception>();

                    foreach (var candidate in candidates)
                    {
                        var selected = SelectType(resolution, candidate);

                        if (cases.ContainsKey(selected.Type))
                        {
                            continue;
                        }

                        var underlying = Nullable.GetUnderlyingType(selected.Type) ?? selected.Type;

                        Expression body;

                        try
                        {
                            var build = typeof(IBinarySerializerBuilder)
                                .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
                                .MakeGenericMethod(underlying);

                            body = Expression.Block(
                                writeIndex(candidate),
                                Expression.Invoke(
                                    Expression.Constant(
                                        build.Invoke(SerializerBuilder, new object[] { candidate, cache }),
                                        typeof(Action<,>).MakeGenericType(underlying, typeof(Stream))
                                    ),
                                    Expression.ConvertChecked(value, underlying),
                                    stream
                                )
                            );
                        }
                        catch (TargetInvocationException indirect)
                        {
                            exceptions.Add(indirect.InnerException);
                            continue;
                        }

                        if (@null != null && !(selected.Type.IsValueType && Nullable.GetUnderlyingType(selected.Type) == null))
                        {
                            body = Expression.IfThenElse(
                                Expression.Equal(value, Expression.Constant(null, selected.Type)),
                                writeIndex(@null),
                                body
                            );
                        }

                        cases.Add(selected.Type, body);
                    }

                    if (cases.Count == 0)
                    {
                        throw new UnsupportedTypeException(
                            source,
                            $"{source.Name} does not match any non-null members of the union [{string.Join(", ", schemas.Select(s => s.GetType().Name))}].",
                            new AggregateException(exceptions)
                        );
                    }

                    if (cases.Count == 1 && cases.First() is var first && first.Key == resolution.Type)
                    {
                        expression = first.Value;
                    }
                    else
                    {
                        var exceptionConstructor = typeof(InvalidOperationException)
                            .GetConstructor(new[] { typeof(string) });

                        expression = Expression.Throw(Expression.New(
                            exceptionConstructor,
                            Expression.Constant($"Unexpected type encountered serializing to {source.Name}.")));

                        foreach (var @case in cases)
                        {
                            expression = Expression.IfThenElse(
                                Expression.TypeIs(value, @case.Key),
                                @case.Value,
                                expression);
                        }
                    }
                }

                // otherwise, we know that the schema is just ["null"]:
                else
                {
                    expression = writeIndex(@null);
                }

                var lambda = Expression.Lambda(expression, "union serializer", new[] { value, stream });
                var compiled = lambda.Compile();

                if (!cache.TryAdd((source, schema), compiled))
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
        /// The resolution to build the child serializer with. The type in the original resolution
        /// must be assignable from the type in the returned resolution.
        /// </returns>
        protected virtual TypeResolution SelectType(TypeResolution resolution, Schema schema)
        {
            return resolution;
        }
    }
}
