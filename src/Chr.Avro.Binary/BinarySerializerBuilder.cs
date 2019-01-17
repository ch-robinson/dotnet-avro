using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
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
        Action<T, Stream> BuildDelegate<T>(Schema schema, IDictionary<(Type, Schema), Delegate> cache = null);

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
        Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache);

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        bool IsMatch(Schema schema);

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        bool IsMatch(TypeResolution resolution);
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
        protected readonly IReadOnlyCollection<IBinarySerializerBuilderCase> Cases;

        /// <summary>
        /// A resolver to obtain type information from.
        /// </summary>
        protected readonly ITypeResolver Resolver;

        /// <summary>
        /// Creates a new serializer builder.
        /// </summary>
        /// <param name="cases">
        /// An optional collection of cases. If no case collection is provided, the default set will
        /// be used.
        /// </param>
        /// <param name="codec">
        /// A codec implementation that generated serializers will use for write operations. If no
        /// codec is provided, <see cref="BinaryCodec" /> will be used.
        /// </param>
        /// <param name="resolver">
        /// A resolver to obtain type information from.
        /// </param>
        public BinarySerializerBuilder(IReadOnlyCollection<IBinarySerializerBuilderCase> cases = null, IBinaryCodec codec = null, ITypeResolver resolver = null)
        {
            Resolver = resolver ?? new DataContractResolver();

            if (codec == null)
            {
                codec = new BinaryCodec();
            }

            Cases = cases ?? new List<IBinarySerializerBuilderCase>()
            {
                // logical types:
                new DecimalSerializerBuilderCase(codec),
                new DurationSerializerBuilderCase(codec),
                new TimestampSerializerBuilderCase(codec),

                // primitives:
                new BooleanSerializerBuilderCase(codec),
                new BytesSerializerBuilderCase(codec),
                new DoubleSerializerBuilderCase(codec),
                new FixedSerializerBuilderCase(codec),
                new FloatSerializerBuilderCase(codec),
                new IntegerSerializerBuilderCase(codec),
                new NullSerializerBuilderCase(),
                new StringSerializerBuilderCase(codec),

                // collections:
                new ArraySerializerBuilderCase(codec, this),
                new MapSerializerBuilderCase(codec, this),

                // enums:
                new EnumSerializerBuilderCase(codec),

                // records:
                new RecordSerializerBuilderCase(this),

                // unions:
                new UnionSerializerBuilderCase(codec, this)
            };
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
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the serializer builder is unable to build a delegate for the schema.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the serializer builder is unable to build a delegate for the type.
        /// </exception>
        public Action<T, Stream> BuildDelegate<T>(Schema schema, IDictionary<(Type, Schema), Delegate> cache = null)
        {
            if (cache == null)
            {
                cache = new Dictionary<(Type, Schema), Delegate>();
            }

            var resolution = Resolver.ResolveType(typeof(T));

            if (cache.TryGetValue((resolution.Type, schema), out var existing))
            {
                return existing as Action<T, Stream>;
            }

            var candidates = Cases.Where(c => c.IsMatch(schema));

            if (candidates.Count() == 0)
            {
                throw new UnsupportedSchemaException(schema, $"No serializer builder case matched {schema.GetType().Name}.");
            }

            var match = candidates.FirstOrDefault(c => c.IsMatch(resolution));

            if (match == null)
            {
                throw new UnsupportedTypeException(resolution.Type, $"No serializer builder case matched {resolution.GetType().Name}.");
            }

            return match.BuildDelegate(resolution, schema, cache) as Action<T, Stream>;
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
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when the serializer builder is unable to build a serializer for the schema.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the serializer builder is unable to build a serializer for the type.
        /// </exception>
        public IBinarySerializer<T> BuildSerializer<T>(Schema schema)
        {
            return new BinarySerializer<T>(BuildDelegate<T>(schema));
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
        protected readonly IBinaryCodec Codec;

        /// <summary>
        /// The serializer builder to use to build item serializers.
        /// </summary>
        protected readonly IBinarySerializerBuilder SerializerBuilder;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not an <see cref="ArraySchema" /> or the resolution is not an
        /// <see cref="ArrayResolution" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is ArrayResolution arrayResolution))
            {
                throw new ArgumentException("An array serializer can only be built for an array resolution.");
            }

            if (!(schema is ArraySchema arraySchema))
            {
                throw new ArgumentException("An array serializer can only be built for an array schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is an <see cref="ArraySchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is ArraySchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="ArrayResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is ArrayResolution;
        }
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
        /// A delegate cache. If a delegate is cached for a specific type-schema pair, that delegate
        /// will be returned for all subsequent occurrences of the pair.
        /// </param>
        /// <returns>
        /// An action that accepts an object and a <see cref="Stream" /> and writes the serialized
        /// object to the stream.
        /// </returns>
        public abstract Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache);

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        public abstract bool IsMatch(Schema schema);

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        public abstract bool IsMatch(TypeResolution resolution);
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
        protected readonly IBinaryCodec Codec;
        
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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="BooleanSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="bool" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is BooleanSchema))
            {
                throw new ArgumentException("A boolean serializer can only be built for a boolean schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="BooleanSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is BooleanSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="bool" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="BytesSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="T:System.Byte[]" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is BytesSchema))
            {
                throw new ArgumentException("A bytes serializer can only be built for a bytes schema.");
            }

            var source = resolution.Type;
            var target = typeof(byte[]);

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
                    throw new UnsupportedTypeException(source, $"A bytes serializer cannot be built for type {source.FullName}.", inner);
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="BytesSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is BytesSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="T:System.Byte[]" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="BytesSchema" /> or a <see cref="FixedSchema "/>
        /// with logical type <see cref="DecimalLogicalType" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="decimal" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema.LogicalType is DecimalLogicalType decimalLogicalType))
            {
                throw new ArgumentException("A decimal deserializer can only be built for schema with a decimal logical type.");
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

            // declare storage variables:
            var bytes = Expression.Variable(typeof(byte[]));
            var integer = Expression.Variable(typeof(BigInteger));

            var integerConstructor = typeof(BigInteger)
                .GetConstructor(new[] { typeof(decimal) });

            var abs = typeof(BigInteger)
                .GetMethod(nameof(BigInteger.Abs), new[] { typeof(BigInteger) });

            var ceil = typeof(Math)
                 .GetMethod(nameof(Math.Ceiling), new[] { typeof(double) });

            var log = typeof(BigInteger)
                .GetMethod(nameof(BigInteger.Log10), new[] { typeof(BigInteger) });

            var max = typeof(Math)
                .GetMethod(nameof(Math.Max), new[] { typeof(double), typeof(double) });

            var pow = typeof(Math)
                .GetMethod(nameof(Math.Pow), new[] { typeof(double), typeof(double) });

            var reverse = typeof(Array)
                .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

            var round = typeof(Math)
                .GetMethod(nameof(Math.Round), new[] { typeof(decimal), typeof(int) });

            var toByteArray = typeof(BigInteger)
                .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

            result = Expression.Block(
                new[] { integer },

                // scale and get the digits:
                //   integer = new BigInteger(Math.Round(result, scale) * (decimal)Math.Pow(10, scale));
                Expression.Assign(integer,
                    Expression.New(integerConstructor,
                        Expression.Multiply(
                            Expression.Call(null, round, result, Expression.Constant(scale)),
                            Expression.Constant((decimal)Math.Pow(10, scale))))),

                // arithmetic:
                //   var digits = Math.Ceiling(BigInteger.Log10(BigInteger.Abs(integer)));
                //   var truncated = integer - (integer % (BigInteger)Math.Pow(10, Math.Max(0, digits - precision)));
                //
                //   bytes = truncated.ToByteArray();
                Expression.Assign(bytes,
                    Expression.Call(
                        Expression.Subtract(integer,
                            Expression.Modulo(integer,
                                Expression.ConvertChecked(
                                    Expression.Call(null, pow, Expression.Constant(10.0),
                                        Expression.Call(null, max, Expression.Constant(0.0),
                                            Expression.Subtract(
                                                Expression.Call(null, ceil,
                                                    Expression.Call(null, log,
                                                        Expression.Call(null, abs, integer))),
                                                Expression.Constant((double)precision)
                                            ))),
                                    typeof(BigInteger)))),
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
                        Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema.Name} (size {fixedSchema.Size}) and decimal with precision {decimalLogicalType.Precision} and scale {decimalLogicalType.Scale}.")))
                    ),
                    Expression.Call(codec, writeValue, bytes, stream)
                );
            }
            else
            {
                throw new ArgumentException("A decimal serializer can only be built for a bytes or a fixed schema.");
            }
            
            var lambda = Expression.Lambda(result, "decimal serializer", new[] { value, stream });
            var compiled = lambda.Compile();
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="BytesSchema" /> or a <see cref="FixedSchema "/> with
        /// logical type <see cref="DecimalLogicalType" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return (schema is BytesSchema || schema is FixedSchema) && schema.LogicalType is DecimalLogicalType;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists from <see cref="decimal" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="DoubleSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="double" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is DoubleSchema))
            {
                throw new ArgumentException("A double serializer can only be built for a double schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="DoubleSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is DoubleSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="double" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="FixedSchema" /> with size 12 and logical
        /// type <see cref="DurationLogicalType" /> or when the type is not <see cref="TimeSpan" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema.LogicalType is DurationLogicalType))
            {
                throw new ArgumentException("A duration deserializer can only be built for a schema with a duration logical type.");
            }

            if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
            {
                throw new ArgumentException("A duration deserializer can only be built for a fixed schema with size 12.");
            }

            var source = resolution.Type;

            if (source != typeof(TimeSpan))
            {
                throw new ArgumentException($"A duration deserializer cannot be built for {source.Name}.");
            }

            Action<uint, Stream> write = (value, stream) =>
            {
                var bytes = BitConverter.GetBytes(value);

                if (!BitConverter.IsLittleEndian)
                {
                    Array.Reverse(bytes);
                }

                Codec.Write(bytes, stream);
            };

            Action<TimeSpan, Stream> result = (value, stream) =>
            {
                var months = 0U;
                var days = Convert.ToUInt32(value.TotalDays);
                var milliseconds = Convert.ToUInt32((ulong)value.TotalMilliseconds - (days * 86400000UL));

                write(months, stream);
                write(days, stream);
                write(milliseconds, stream);
            };

            cache.Add((source, schema), result);

            return result;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="FixedSchema" /> with size 12 and logical type
        /// <see cref="DurationLogicalType" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is FixedSchema fixedSchema && fixedSchema.LogicalType is DurationLogicalType && fixedSchema.Size == 12;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Whether the type is <see cref="TimeSpan" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution.Type == typeof(TimeSpan);
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not an <see cref="EnumSchema" /> or the resolution is not an
        /// <see cref="EnumResolution" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the schema does not contain a matching symbol for each symbol in the type.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is EnumResolution enumResolution))
            {
                throw new ArgumentException("An enum serializer can only be built for an enum resolution.");
            }

            if (!(schema is EnumSchema enumSchema))
            {
                throw new ArgumentException("An enum serializer can only be built for an enum schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is an <see cref="EnumSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is EnumSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="EnumResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is EnumResolution;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="FixedSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="T:System.Byte[]" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is FixedSchema fixedSchema))
            {
                throw new ArgumentException("A fixed serializer can only be built for a fixed schema.");
            }

            var source = resolution.Type;
            var target = typeof(byte[]);

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
                    throw new UnsupportedTypeException(source, $"A fixed serializer cannot be built for type {source.FullName}.", inner);
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="FixedSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is FixedSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="T:System.Byte[]" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="FloatSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="float" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is FloatSchema))
            {
                throw new ArgumentException("A float serializer can only be built for a float schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="FloatSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is FloatSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="float" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not an <see cref="IntSchema" /> or a <see cref="LongSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="long" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is IntSchema || schema is LongSchema))
            {
                throw new ArgumentException("An integer serializer can only be built for an int or long schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is an <see cref="IntSchema" /> or a <see cref="LongSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is IntSchema || schema is LongSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="long" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

        /// <summary>
        /// The serializer builder to use to build key and value serializers.
        /// </summary>
        protected readonly IBinarySerializerBuilder SerializerBuilder;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not an <see cref="MapSchema" /> or the resolution is not an
        /// <see cref="MapResolution" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type is not a <see cref="KeyValuePair{TKey, TValue}" /> enumerable.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is MapResolution mapResolution))
            {
                throw new ArgumentException("A map serializer can only be built for a map resolution.");
            }

            if (!(schema is MapSchema mapSchema))
            {
                throw new ArgumentException("A map serializer can only be built for a map schema.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is an <see cref="MapSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is MapSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="MapResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is MapResolution;
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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="NullSchema" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is NullSchema))
            {
                throw new ArgumentException("A null serializer can only be built for a null schema.");
            }

            var source = resolution.Type;

            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);
            
            var lambda = Expression.Lambda(Expression.Empty(), "null serializer", new[] { value, stream });
            var compiled = lambda.Compile();
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="NullSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is NullSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; the null serializer performs no operations.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinarySerializerBuilder SerializerBuilder;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="RecordSchema" /> or the resolution is not a
        /// <see cref="RecordResolution" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(resolution is RecordResolution recordResolution))
            {
                throw new ArgumentException("A record serializer can only be built for a record resolution.");
            }

            if (!(schema is RecordSchema recordSchema))
            {
                throw new ArgumentException("A record serializer can only be built for a record schema.");
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
            var compiled = lambda.Compile();
            cache.Add((source, schema), compiled);

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
            });

            result = Expression.Block(typeof(void), writes);
            lambda = Expression.Lambda(result, $"{recordSchema.Name} field writer", new[] { value, stream });
            write = lambda.Compile();

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="RecordSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is RecordSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="RecordResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is RecordResolution;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="StringSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no conversion to <see cref="string" /> exists.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is StringSchema))
            {
                throw new ArgumentException("A string serializer can only be built for a string schema.");
            }

            var source = resolution.Type;
            var target = typeof(string);

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            Expression result = value;

            if (source != target)
            {
                if (source == typeof(DateTime) || source == typeof(DateTimeOffset))
                {
                    if (source == typeof(DateTimeOffset))
                    {
                        result = Expression.PropertyOrField(result, nameof(DateTimeOffset.UtcDateTime));
                    }

                    var convertDateTime = typeof(DateTime)
                        .GetMethod(nameof(DateTime.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                    result = Expression.Call(
                        result,
                        convertDateTime,
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="StringSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is StringSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if no conversion exists to <see cref="string" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
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
        protected readonly IBinaryCodec Codec;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="LongSchema" /> with logical type
        /// <see cref="MicrosecondTimestampLogicalType" /> or <see cref="MillisecondTimestampLogicalType" />
        /// or the type is not <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is LongSchema))
            {
                throw new ArgumentException("A timestamp serializer can only be built for a long schema.");
            }
            
            var source = resolution.Type;

            if (source != typeof(DateTime) && source != typeof(DateTimeOffset))
            {
                throw new ArgumentException($"A timestamp serializer cannot be built for {source.Name}.");
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
                throw new ArgumentException("A timestamp serializer can only be built for a schema with a timestamp logical type.");
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
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="LongSchema" /> with logical type
        /// <see cref="MicrosecondTimestampLogicalType" /> or <see cref="MillisecondTimestampLogicalType" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is LongSchema longSchema && (longSchema.LogicalType is MicrosecondTimestampLogicalType || longSchema.LogicalType is MillisecondTimestampLogicalType);
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Whether the type is <see cref="DateTime" /> or <see cref="DateTimeOffset" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution.Type == typeof(DateTime) || resolution.Type == typeof(DateTimeOffset);
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
        protected readonly IBinaryCodec Codec;

        /// <summary>
        /// The serializer builder to use to build child serializers.
        /// </summary>
        protected readonly IBinarySerializerBuilder SerializerBuilder;

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
        /// <exception cref="ArgumentException">
        /// Thrown when the schema is not a <see cref="UnionSchema" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the type cannot be mapped to at least one schema in the union.
        /// </exception>
        public override Delegate BuildDelegate(TypeResolution resolution, Schema schema, IDictionary<(Type, Schema), Delegate> cache)
        {
            if (!(schema is UnionSchema unionSchema))
            {
                throw new ArgumentException("A union serializer can only be built for a union schema.");
            }

            var schemas = unionSchema.Schemas.ToList();
            var source = resolution.Type;

            var codec = Expression.Constant(Codec);
            var stream = Expression.Parameter(typeof(Stream));
            var value = Expression.Parameter(source);

            var writeIndex = typeof(IBinaryCodec)
                .GetMethod(nameof(IBinaryCodec.WriteInteger));

            Expression result = null;

            // generate a write function for the first matching non-null schema:
            for (var i = 0; i < schemas.Count; i++)
            {
                if (schemas[i] is var candidate && candidate is NullSchema)
                {
                    continue;
                }

                var underlying = Nullable.GetUnderlyingType(source) ?? source;

                try
                {
                    var build = typeof(IBinarySerializerBuilder)
                        .GetMethod(nameof(IBinarySerializerBuilder.BuildDelegate))
                        .MakeGenericMethod(underlying);
                    
                    result = Expression.Constant(
                        build.Invoke(SerializerBuilder, new object[] { candidate, cache }),
                        typeof(Action<,>).MakeGenericType(underlying, typeof(Stream))
                    );
                }
                catch (TargetInvocationException)
                {
                    continue;
                }

                result = Expression.Block(
                    Expression.Call(codec, writeIndex, Expression.Constant((long)i), stream),
                    Expression.Invoke(result, Expression.ConvertChecked(value, underlying), stream)
                );

                break;
            }

            var nullIndex = schemas.FindIndex(s => s is NullSchema);

            if (nullIndex < 0 || (source.IsValueType && Nullable.GetUnderlyingType(source) == null))
            {
                if (result == null)
                {
                    throw new UnsupportedTypeException(source, $"{source.Name} cannot be serialized to the union [{string.Join(", ", schemas.Select(s => s.GetType().Name))}].");
                }
            }
            else
            {
                Expression writeNull = Expression.Call(codec, writeIndex, Expression.Constant((long)nullIndex), stream);

                result = result == null
                    ? writeNull
                    : Expression.IfThenElse(
                        Expression.Equal(value, Expression.Constant(null, source)),
                        writeNull,
                        result
                    );
            }

            var lambda = Expression.Lambda(result, "union serializer", new[] { value, stream });
            var compiled = lambda.Compile();
            cache.Add((source, schema), compiled);

            return compiled;
        }

        /// <summary>
        /// Determines whether the case can be applied to a schema.
        /// </summary>
        /// <returns>
        /// Whether the schema is a <see cref="UnionSchema" />.
        /// </returns>
        public override bool IsMatch(Schema schema)
        {
            return schema is UnionSchema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a type resolution.
        /// </summary>
        /// <returns>
        /// Always true; this case will apply but fail if the type cannot be mapped to at least one
        /// schema in the union.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return true;
        }
    }
}
