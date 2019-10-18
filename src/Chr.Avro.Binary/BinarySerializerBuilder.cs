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
        Action<T, Stream> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache = null);

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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);
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
        /// A resolver to obtain type information from.
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
        /// A resolver to obtain type information from.
        /// </param>
        public BinarySerializerBuilder(IBinaryCodec codec = null, ITypeResolver resolver = null)
            : this(CreateBinarySerializerCaseBuilders(codec ?? new BinaryCodec()), resolver) { }

        /// <summary>
        /// Creates a new serializer builder.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolver">
        /// A resolver to obtain type information from.
        /// </param>
        public BinarySerializerBuilder(IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> caseBuilders, ITypeResolver resolver = null)
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
        /// <exception cref="AggregateException">
        /// Thrown when no case matches the schema or type. <see cref="AggregateException.InnerExceptions" />
        /// will be contain the exceptions thrown by each case.
        /// </exception>
        public Action<T, Stream> BuildDelegate<T>(Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache = null)
        {
            if (cache == null)
            {
                cache = new ConcurrentDictionary<(Type, Schema), Delegate>();
            }

            var resolution = Resolver.ResolveType(typeof(T));

            if (cache.TryGetValue((resolution.Type, schema), out var existing))
            {
                return existing as Action<T, Stream>;
            }

            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                try
                {
                    return @case.BuildDelegate(resolution, schema, cache) as Action<T, Stream>;
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

            throw new AggregateException($"No serializer builder case matched {resolution.GetType().Name}.", exceptions);
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
        /// <exception cref="AggregateException">
        /// Thrown when no case matches the schema or type. <see cref="AggregateException.InnerExceptions" />
        /// will be contain the exceptions thrown by each case.
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
    /// A base serializer builder case.
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
        /// A delegate cache. If a delegate is cached for a specific type-schema pair, that delegate
        /// will be returned for all subsequent occurrences of the pair.
        /// </param>
        /// <returns>
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream. Since this is not a typed method, the general <see cref="Delegate" />
        /// type is used.
        /// </returns>
        public abstract Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache);
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
            Codec = codec;
            SerializerBuilder = serializerBuilder;
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an <see cref="ArraySchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not an <see cref="ArrayResolution" /> or the resolved
        /// type does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is ArrayResolution arrayResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "An array serializer can only be built for an array resolution.");
            }

            if (!(schema is ArraySchema arraySchema))
            {
                throw new UnsupportedSchemaException(schema, "An array serializer can only be built for an array schema.");
            }

            var source = arrayResolution.Type;
            var item = arrayResolution.ItemType;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = null;

            try
            {
                var build = typeof(IBinarySerializerBuilder)
                    .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
                    .MakeGenericMethod(item);

                result = Expression.Constant(
                    build.Invoke(SerializerBuilder, new object[] { arraySchema.Item, cache }),
                    typeof(Action<,>).MakeGenericType(item, typeof(Stream))
                );
            }
            catch (TargetInvocationException indirect)
            {
                ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
            }

            try
            {
                var writeBlocks = typeof(IBinaryCodec)
                    .GetMethods()
                    .Single(m => m.Name == nameof(IBinaryCodec.WriteBlocks)
                        && m.GetGenericArguments().Length == 1
                    )
                    .MakeGenericMethod(item);

                result = Expression.Call(codec, writeBlocks, value, result, stream);
            }
            catch (InvalidOperationException inner)
            {
                throw new UnsupportedTypeException(source, $"An array serializer cannot be built for type {source.FullName}.", inner);
            }

            var lambda = Expression.Lambda(result, "array serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BooleanSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="bool" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is BooleanSchema))
            {
                throw new UnsupportedSchemaException(schema, "A boolean serializer can only be built for a boolean schema.");
            }

            var source = resolution.Type;
            var target = typeof(bool);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(source, $"A boolean serializer cannot be built for type {source.FullName}.", inner);
                }
            }

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteBoolean));

            result = Expression.Call(codec, writeValue, result, stream);

            var lambda = Expression.Lambda(result, "boolean serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BytesSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="T:System.Byte[]" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is BytesSchema))
            {
                throw new UnsupportedSchemaException(schema, "A bytes serializer can only be built for a bytes schema.");
            }

            var source = resolution.Type;
            var target = typeof(byte[]);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                if (source == typeof(Guid))
                {
                    var convertGuid = typeof(Guid)
                        .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                    result = Expression.Call(result, convertGuid);
                }
                else
                {
                    try
                    {
                        result = Expression.ConvertChecked(result, target);
                    }
                    catch (InvalidOperationException inner)
                    {
                        throw new UnsupportedTypeException(source, $"A bytes serializer cannot be built for type {source.FullName}.", inner);
                    }
                }
            }

            var writeLength = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteInteger));

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Write));

            result = Expression.Block(
                Expression.Call(codec, writeLength, Expression.ConvertChecked(Expression.ArrayLength(result), typeof(long)), stream),
                Expression.Call(codec, writeValue, result, stream)
            );

            var lambda = Expression.Lambda(result, "bytes serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="DoubleSchema" /> and attempts to map it
    /// to any provided type.
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="BytesSchema" /> or a <see cref="FixedSchema "/>
        /// with logical type <see cref="DecimalLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="decimal" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema.LogicalType is DecimalLogicalType decimalLogicalType))
            {
                throw new UnsupportedSchemaException(schema, "A decimal deserializer can only be built for schema with a decimal logical type.");
            }

            var precision = decimalLogicalType.Precision;
            var scale = decimalLogicalType.Scale;
            var source = resolution.Type;
            var target = typeof(decimal);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(source, $"A decimal serializer cannot be built for type {source.FullName}.", inner);
                }
            }

            // buffer:
            var bytes = Expression.Variable(typeof(byte[]));

            var integerConstructor = typeof(BigInteger)
                .GetConstructor(new[] { typeof(decimal) });

            var reverse = typeof(Array)
                .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

            var toByteArray = typeof(BigInteger)
                .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

            result = Expression.Block(
                // bytes = new BigInteger(result * (decimal)Math.Pow(10, scale)).ToByteArray();
                Expression.Assign(bytes,
                    Expression.Call(
                        Expression.New(integerConstructor,
                            Expression.Multiply(
                                result,
                                Expression.Constant((decimal)Math.Pow(10, scale)))),
                        toByteArray)),

                // BigInteger is little-endian, so reverse:
                Expression.Call(null, reverse, bytes),

                // return byte array:
                bytes
            );

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Write));

            // figure out how to write:
            if (schema is BytesSchema)
            {
                var writeLength = typeof(IBinaryCodec)
                    .GetMethod(nameof(IBinaryCodec.WriteInteger));

                result = Expression.Block(
                    new[] { bytes },
                    result,
                    Expression.Call(codec, writeLength, Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), stream),
                    Expression.Call(codec, writeValue, bytes, stream)
                );
            }
            else if (schema is FixedSchema fixedSchema)
            {
                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                result = Expression.Block(
                    new[] { bytes },
                    result,
                    Expression.IfThen(
                        Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                        Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema.Name} (size {fixedSchema.Size}) and decimal with precision {precision} and scale {scale}.")))
                    ),
                    Expression.Call(codec, writeValue, bytes, stream)
                );
            }
            else
            {
                throw new UnsupportedSchemaException(schema, "A decimal serializer can only be built for a bytes or a fixed schema.");
            }

            var lambda = Expression.Lambda(result, "decimal serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="DoubleSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="double" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is DoubleSchema))
            {
                throw new UnsupportedSchemaException(schema, "A double serializer can only be built for a double schema.");
            }

            var source = resolution.Type;
            var target = typeof(double);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(source, $"A double serializer cannot be built for type {source.FullName}.", inner);
                }
            }

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteDouble));

            result = Expression.Call(codec, writeValue, result, stream);

            var lambda = Expression.Lambda(result, "double serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
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

            var source = resolution.Type;

            if (source != typeof(TimeSpan))
            {
                throw new UnsupportedTypeException(source, $"A duration deserializer cannot be built for {source.Name}.");
            }

            void write(uint value, Stream stream)
            {
                var bytes = BitConverter.GetBytes(value);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                Codec.Write(bytes, stream);
            }

            Action<TimeSpan, Stream> result = (value, stream) =>
            {
                var months = 0U;
                var days = Convert.ToUInt32(value.TotalDays);
                var milliseconds = Convert.ToUInt32((ulong)value.TotalMilliseconds - (days * 86400000UL));

                write(months, stream);
                write(days, stream);
                write(milliseconds, stream);
            };

            return cache.GetOrAdd((source, schema), result);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an <see cref="EnumSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not an <see cref="EnumResolution" /> or the schema does
        /// not contain a matching symbol for each symbol in the type.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is EnumResolution enumResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "An enum serializer can only be built for an enum resolution.");
            }

            if (!(schema is EnumSchema enumSchema))
            {
                throw new UnsupportedSchemaException(schema, "An enum serializer can only be built for an enum schema.");
            }

            var source = resolution.Type;
            var symbols = enumSchema.Symbols.ToList();

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            var writeIndex = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteInteger));

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

                var write = Expression.Call(
                    codec,
                    writeIndex,
                    Expression.Constant((long)index),
                    stream
                );

                return Expression.SwitchCase(write, Expression.Constant(symbol.Value));
            });

            var result = Expression.Switch(value, cases.ToArray());
            var lambda = Expression.Lambda(result, $"{enumSchema.Name} serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FixedSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="T:System.Byte[]" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is FixedSchema fixedSchema))
            {
                throw new UnsupportedSchemaException(schema, "A fixed serializer can only be built for a fixed schema.");
            }

            var source = resolution.Type;
            var target = typeof(byte[]);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                if (source == typeof(Guid))
                {
                    if (fixedSchema.Size != 16)
                    {
                        throw new UnsupportedSchemaException(schema, $"A fixed schema cannot be mapped to a Guid unless its size is 16.");
                    }

                    var convertGuid = typeof(Guid)
                        .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                    result = Expression.Call(result, convertGuid);
                }
                else
                {
                    try
                    {
                        result = Expression.ConvertChecked(result, target);
                    }
                    catch (InvalidOperationException inner)
                    {
                        throw new UnsupportedTypeException(source, $"A fixed serializer cannot be built for type {source.FullName}.", inner);
                    }
                }
            }

            var exceptionConstructor = typeof(OverflowException)
                .GetConstructor(new[] { typeof(string) });

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Write));

            result = Expression.Block(
                Expression.IfThen(
                    Expression.NotEqual(Expression.ArrayLength(result), Expression.Constant(fixedSchema.Size)),
                    Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Only arrays of size {fixedSchema.Size} can be serialized to {fixedSchema.Name}.")))
                ),
                Expression.Call(codec, writeValue, result, stream)
            );

            var lambda = Expression.Lambda(result, $"{fixedSchema.Name} serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="FloatSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="float" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is FloatSchema))
            {
                throw new UnsupportedSchemaException(schema, "A float serializer can only be built for a float schema.");
            }

            var source = resolution.Type;
            var target = typeof(float);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(source, $"A float serializer cannot be built for type {source.FullName}.", inner);
                }
            }

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteSingle));

            result = Expression.Call(codec, writeValue, result, stream);

            var lambda = Expression.Lambda(result, "float serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not an <see cref="IntSchema" /> or a <see cref="LongSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="long" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is IntSchema || schema is LongSchema))
            {
                throw new UnsupportedSchemaException(schema, "An integer serializer can only be built for an int or long schema.");
            }

            var source = resolution.Type;
            var target = typeof(long);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                try
                {
                    result = Expression.ConvertChecked(result, target);
                }
                catch (InvalidOperationException inner)
                {
                    throw new UnsupportedTypeException(source, $"An integer serializer cannot be built for type {source.FullName}.", inner);
                }
            }

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteInteger));

            result = Expression.Call(codec, writeValue, result, stream);

            var lambda = Expression.Lambda(result, "integer serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
            Codec = codec;
            SerializerBuilder = serializerBuilder;
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="MapSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolution is not a <see cref="MapResolution" /> or the resolved type
        /// is not a <see cref="KeyValuePair{TKey, TValue}" /> enumerable.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is MapResolution mapResolution))
            {
                throw new UnsupportedTypeException(resolution.Type, "A map serializer can only be built for a map resolution.");
            }

            if (!(schema is MapSchema mapSchema))
            {
                throw new UnsupportedSchemaException(schema, "A map serializer can only be built for a map schema.");
            }

            var source = mapResolution.Type;
            var key = mapResolution.KeyType;
            var item = mapResolution.ValueType;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            try
            {
                var build = typeof(IBinarySerializerBuilder)
                    .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate));

                var buildKey = build.MakeGenericMethod(key);
                var buildItem = build.MakeGenericMethod(item);

                var writeBlocks = typeof(IBinaryCodec)
                    .GetMethods()
                    .Single(m => m.Name == nameof(IBinaryCodec.WriteBlocks)
                        && m.GetGenericArguments().Length == 2
                    )
                    .MakeGenericMethod(key, item);

                result = Expression.Call(
                    codec,
                    writeBlocks,
                    result,
                    Expression.Constant(
                        buildKey.Invoke(SerializerBuilder, new object[] { new StringSchema(), cache }),
                        typeof(Action<,>).MakeGenericType(key, typeof(Stream))
                    ),
                    Expression.Constant(
                        buildItem.Invoke(SerializerBuilder, new object[] { mapSchema.Value, cache }),
                        typeof(Action<,>).MakeGenericType(item, typeof(Stream))
                    ),
                    stream
                );
            }
            catch (TargetInvocationException indirect)
            {
                ExceptionDispatchInfo.Capture(indirect.InnerException).Throw();
            }

            var lambda = Expression.Lambda(result, "map serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and does nothing.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="NullSchema" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is NullSchema))
            {
                throw new UnsupportedSchemaException(schema, "A null serializer can only be built for a null schema.");
            }

            var source = resolution.Type;

            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            var lambda = Expression.Lambda(Expression.Empty(), "null serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
            SerializerBuilder = serializerBuilder;
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
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
                throw new UnsupportedTypeException(resolution.Type, "A record serializer can only be built for a record resolution.");
            }

            if (!(schema is RecordSchema recordSchema))
            {
                throw new UnsupportedSchemaException(schema, "A record serializer can only be built for a record schema.");
            }

            var source = resolution.Type;

            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            // declare an action that writes the record fields in order:
            Delegate write = null;

            // bind to this scope:
            Expression result = Expression.Invoke(Expression.Constant((Func<Delegate>)(() => write)));

            // coerce Delegate to Action<TSource, Stream>:
            result = Expression.ConvertChecked(result, typeof(Action<,>).MakeGenericType(source, typeof(Stream)));

            // serialize the record:
            result = Expression.Invoke(result, value, stream);

            var lambda = Expression.Lambda(result, $"{recordSchema.Name} serializer", new[] { value, stream });
            var compiled = cache.GetOrAdd((source, schema), lambda.Compile());

            // now that an infinite cycle won’t happen, build the write function:
            var writes = recordSchema.Fields.Select(field =>
            {
                var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));

                if (match == null)
                {
                    throw new UnsupportedTypeException(source, $"{source.FullName} does not have a field or property that matches the {field.Name} field on {recordSchema.Name}.");
                }

                var type = match.Type;

                Expression action = null;

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

            result = writes.Count > 0 ? Expression.Block(typeof(void), writes) : Expression.Empty() as Expression;
            lambda = Expression.Lambda(result, $"{recordSchema.Name} field writer", new[] { value, stream });
            write = lambda.Compile();

            return compiled;
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="StringSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="string" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is StringSchema))
            {
                throw new UnsupportedSchemaException(schema, "A string serializer can only be built for a string schema.");
            }

            var source = resolution.Type;
            var target = typeof(string);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                if (source == typeof(DateTime))
                {
                    var convertDateTime = typeof(DateTime)
                        .GetMethod(nameof(DateTime.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                    result = Expression.Call(
                        result,
                        convertDateTime,
                        Expression.Constant("O"),
                        Expression.Constant(CultureInfo.InvariantCulture)
                    );
                }
                else if (source == typeof(DateTimeOffset))
                {
                    var convertDateTimeOffset = typeof(DateTimeOffset)
                        .GetMethod(nameof(DateTimeOffset.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                    result = Expression.Call(
                        result,
                        convertDateTimeOffset,
                        Expression.Constant("O"),
                        Expression.Constant(CultureInfo.InvariantCulture)
                    );
                }
                else if (source == typeof(Guid))
                {
                    var convertGuid = typeof(Guid)
                        .GetMethod(nameof(Guid.ToString), Type.EmptyTypes);

                    result = Expression.Call(result, convertGuid);
                }
                else if (source == typeof(TimeSpan))
                {
                    var convertTimeSpan = typeof(XmlConvert)
                        .GetMethod(nameof(XmlConvert.ToString), new[] { typeof(TimeSpan) });

                    result = Expression.Call(null, convertTimeSpan, result);
                }
                else if (source == typeof(Uri))
                {
                    var convertUri = typeof(Uri)
                        .GetMethod(nameof(Uri.ToString));

                    result = Expression.Call(result, convertUri);
                }
                else
                {
                    try
                    {
                        result = Expression.ConvertChecked(result, target);
                    }
                    catch (InvalidOperationException inner)
                    {
                        throw new UnsupportedTypeException(source, $"A string serializer cannot be built for type {source.FullName}.", inner);
                    }
                }
            }

            var convertString = typeof(Encoding)
                .GetMethod(nameof(Encoding.GetBytes), new[] { typeof(string) });

            result = Expression.Call(Expression.Constant(Encoding.UTF8), convertString, result);

            var writeLength = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteInteger));

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.Write));

            result = Expression.Block(
                Expression.Call(codec, writeLength, Expression.ConvertChecked(Expression.ArrayLength(result), typeof(long)), stream),
                Expression.Call(codec, writeValue, result, stream)
            );

            var lambda = Expression.Lambda(result, "string serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
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
                throw new UnsupportedSchemaException(schema, "A timestamp serializer can only be built for a long schema.");
            }

            var source = resolution.Type;

            if (source != typeof(DateTime) && source != typeof(DateTimeOffset))
            {
                throw new UnsupportedTypeException(source, $"A timestamp serializer cannot be built for {source.Name}.");
            }

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = Expression.ConvertChecked(value, typeof(DateTimeOffset));

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
                throw new UnsupportedSchemaException(schema, "A timestamp serializer can only be built for a schema with a timestamp logical type.");
            }

            var utcTicks = typeof(DateTimeOffset)
                .GetProperty(nameof(DateTimeOffset.UtcTicks));

            // result = (value.UtcTicks - epoch) / factor;
            result = Expression.Divide(Expression.Subtract(Expression.Property(result, utcTicks), epoch), factor);

            var writeValue = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteInteger));

            result = Expression.Call(codec, writeValue, result, stream);

            var lambda = Expression.Lambda(result, "timestamp serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
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
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the schema is not a <see cref="UnionSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type cannot be mapped to at least one schema in the union.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, ConcurrentDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is UnionSchema unionSchema && unionSchema.Schemas.Count > 0))
            {
                throw new UnsupportedSchemaException(schema, "A union serializer can only be built for a union schema of one or more schemas.");
            }

            var schemas = unionSchema.Schemas.ToList();
            var candidates = schemas.Where(s => !(s is NullSchema)).ToList();
            var @null = schemas.Find(s => s is NullSchema);
            var source = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression writeIndex(Schema child) => Expression.Call(
                codec,
                typeof(IBinaryCodec).GetMethod(nameof(IBinaryCodec.WriteInteger)),
                Expression.Constant((long)schemas.IndexOf(child)),
                stream
            );

            Expression result = null;

            // if there are non-null schemas, select the first matching one:
            if (candidates.Count > 0)
            {
                var exceptions = new List<Exception>();
                var underlying = Nullable.GetUnderlyingType(source) ?? source;

                foreach (var candidate in candidates)
                {
                    try
                    {
                        var build = typeof(IBinarySerializerBuilder)
                            .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
                            .MakeGenericMethod(underlying);

                        result = Expression.Block(
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

                    if (@null != null && !(source.IsValueType && Nullable.GetUnderlyingType(source) == null))
                    {
                        result = Expression.IfThenElse(
                            Expression.Equal(value, Expression.Constant(null, source)),
                            writeIndex(@null),
                            result
                        );
                    }

                    break;
                }

                if (result == null)
                {
                    throw new UnsupportedTypeException(
                        source,
                        $"{source.Name} does not match any non-null members of the union [{string.Join(", ", schemas.Select(s => s.GetType().Name))}].",
                        new AggregateException(exceptions)
                    );
                }
            }

            // otherwise, we know that the schema is just ["null"]:
            else
            {
                result = writeIndex(@null);
            }

            var lambda = Expression.Lambda(result, "union serializer", new[] { value, stream });
            var compiled = lambda.Compile();

            return cache.GetOrAdd((source, schema), compiled);
        }
    }
}
