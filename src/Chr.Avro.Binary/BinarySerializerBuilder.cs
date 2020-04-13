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
        Action<T, Stream> BuildDelegate<T>(Schema schema);

        /// <summary>
        /// Builds an expression that represents writing <paramref name="value" /> to a stream
        /// (provided by <paramref name="context" />).
        /// </summary>
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="schema">
        /// The schema to map to <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        Expression BuildExpression(Expression value, Schema schema, IBinarySerializerBuilderContext context);

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
        /// Any exceptions related to the applicability of the case. If <see cref="Expression" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        ICollection<Exception> Exceptions { get; }

        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        Expression? Expression { get; }
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context);
    }

    /// <summary>
    /// An object that contains information to build a top-level serialization function.
    /// </summary>
    public interface IBinarySerializerBuilderContext
    {
        /// <summary>
        /// A map of top-level variables to their values.
        /// </summary>
        ConcurrentDictionary<ParameterExpression, Expression> Assignments { get; }

        /// <summary>
        /// A map of schema-type pairs to top-level variables.
        /// </summary>
        ConcurrentDictionary<(Schema, Type), ParameterExpression> References { get; }

        /// <summary>
        /// The output <see cref="System.IO.Stream" />.
        /// </summary>
        ParameterExpression Stream { get; }
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
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the type to the schema.
        /// </exception>
        public virtual Action<T, Stream> BuildDelegate<T>(Schema schema)
        {
            var context = new BinarySerializerBuilderContext();
            var value = Expression.Parameter(typeof(T));

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(value, schema, context);

            return Expression.Lambda<Action<T, Stream>>(Expression.Block(
                context.Assignments.Keys,
                context.Assignments
                    .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                    .Concat(new[] { root })
            ), new[] { value, context.Stream }).Compile();
        }

        /// <summary>
        /// Builds an expression that represents writing <paramref name="value" /> to a stream
        /// (provided by <paramref name="context" />).
        /// </summary>
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="schema">
        /// The schema to map to <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <paramref name="value" /> to the schema.
        /// </exception>
        public virtual Expression BuildExpression(Expression value, Schema schema, IBinarySerializerBuilderContext context)
        {
            var resolution = Resolver.ResolveType(value.Type);
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.BuildExpression(value, resolution, schema, context);

                if (result.Expression != null)
                {
                    return result.Expression;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnsupportedTypeException(resolution.Type, $"No serializer builder case matched {resolution.GetType().Name}.", new AggregateException(exceptions));
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
        /// Any exceptions related to the applicability of the case. If <see cref="Expression" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        public ICollection<Exception> Exceptions { get; set; } = new List<Exception>();

        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        public Expression? Expression { get; set; }
    }

    /// <summary>
    /// A base <see cref="IBinarySerializerBuilderCase" /> implementation.
    /// </summary>
    public abstract class BinarySerializerBuilderCase : IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a serializer for a type-schema pair.
        /// </summary>
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        public abstract IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context);

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
                throw new UnsupportedTypeException(intermediate, $"Failed to generate a conversion for {input.Type}.", inner);
            }
        }
    }

    /// <summary>
    /// A base <see cref="IBinarySerializerBuilderContext" /> implementation.
    /// </summary>
    public class BinarySerializerBuilderContext : IBinarySerializerBuilderContext
    {
        /// <summary>
        /// A map of top-level variables to their values.
        /// </summary>
        public ConcurrentDictionary<ParameterExpression, Expression> Assignments { get; }

        /// <summary>
        /// A map of types to top-level variables.
        /// </summary>
        public ConcurrentDictionary<(Schema, Type), ParameterExpression> References { get; }

        /// <summary>
        /// The output <see cref="System.IO.Stream" />.
        /// </summary>
        public ParameterExpression Stream { get; }

        /// <summary>
        /// Creates a new context.
        /// </summary>
        /// <param name="stream">
        /// The output <see cref="System.IO.Stream" />. If an expression is not provided, one will
        /// be created.
        /// </param>
        public BinarySerializerBuilderContext(ParameterExpression? stream = null)
        {
            Assignments = new ConcurrentDictionary<ParameterExpression, Expression>();
            References = new ConcurrentDictionary<(Schema, Type), ParameterExpression>();
            Stream = stream ?? Expression.Parameter(typeof(Stream));
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is an <see cref="ArrayResolution" /> and the
        /// schema is an <see cref="ArraySchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var itemVariable = Expression.Variable(arrayResolution.ItemType);
                    var itemSerializer = SerializerBuilder.BuildExpression(itemVariable, arraySchema.Item, context);

                    result.Expression = Codec.WriteArray(value, itemVariable, itemSerializer, context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="BooleanSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="bool" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is BooleanSchema)
            {
                result.Expression = Codec.WriteBoolean(GenerateConversion(value, typeof(bool)), context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="BytesSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is BytesSchema)
            {
                var bytes = Expression.Variable(typeof(byte[]));

                result.Expression = Expression.Block(
                    new[] { bytes },
                    Expression.Assign(bytes, GenerateConversion(value, bytes.Type)),
                    Codec.WriteInteger(Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), context.Stream),
                    Codec.Write(bytes, context.Stream)
                );
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
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
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;

                var expression = GenerateConversion(value, typeof(decimal));

                // declare variables for in-place transformation:
                var bytes = Expression.Variable(typeof(byte[]));

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(decimal) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                var toByteArray = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

                expression = Expression.Block(
                    Expression.Assign(bytes,
                        Expression.Call(
                            Expression.Add(
                                Expression.Multiply(
                                    Expression.New(
                                        integerConstructor,
                                        expression),
                                    Expression.Constant(BigInteger.Pow(10, scale))),
                                Expression.New(
                                    integerConstructor,
                                    Expression.Multiply(
                                        Expression.Modulo(expression, Expression.Constant(1m)),
                                        Expression.Constant((decimal)Math.Pow(10, scale))))),
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
                        Codec.WriteInteger(Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), context.Stream),
                        Codec.Write(bytes, context.Stream)
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
                        Codec.Write(bytes, context.Stream)
                    );
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                result.Expression = expression;
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="DoubleSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="double" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is DoubleSchema)
            {
                result.Expression = Codec.WriteFloat(GenerateConversion(value, typeof(double)), context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
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
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema.LogicalType is DurationLogicalType)
            {
                if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                {
                    throw new UnsupportedSchemaException(schema);
                }

                if (resolution.Type != typeof(TimeSpan))
                {
                    throw new UnsupportedTypeException(resolution.Type);
                }

                Expression write(Expression value)
                {
                    var getBytes = typeof(BitConverter)
                        .GetMethod(nameof(BitConverter.GetBytes), new[] { value.Type });

                    Expression bytes = Expression.Call(null, getBytes, value);

                    if (!BitConverter.IsLittleEndian)
                    {
                        var buffer = Expression.Variable(bytes.Type);
                        var reverse = typeof(Array)
                            .GetMethod(nameof(Array.Reverse), new[] { bytes.Type });

                        bytes = Expression.Block(
                            new[] { buffer },
                            Expression.Assign(buffer, bytes),
                            Expression.Call(null, reverse, buffer),
                            buffer);
                    }

                    var write = typeof(Stream)
                        .GetMethod(nameof(Stream.Write), new[] { bytes.Type, typeof(int), typeof(int) });

                    return Expression.Call(context.Stream, write, bytes, Expression.Constant(0), Expression.ArrayLength(bytes));
                }

                var totalDays = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalDays));
                var totalMs = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalMilliseconds));

                result.Expression = Expression.Block(
                    write(Expression.Constant(0U)),
                    write(
                        Expression.ConvertChecked(Expression.Property(value, totalDays), typeof(uint))),
                    write(
                        Expression.ConvertChecked(
                            Expression.Subtract(
                                Expression.Convert(Expression.Property(value, totalMs), typeof(ulong)),
                                Expression.Multiply(
                                    Expression.Convert(Expression.Property(value, totalDays), typeof(ulong)),
                                    Expression.Constant(86400000UL))),
                        typeof(uint))));
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is an <see cref="EnumResolution" /> and the
        /// schema is an <see cref="EnumSchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the schema does not contain a matching symbol for each symbol in the type.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var symbols = enumSchema.Symbols.ToList();

                    // find a match for each enum in the type:
                    var cases = enumResolution.Symbols.Select(symbol =>
                    {
                        var index = symbols.FindIndex(s => symbol.Name.IsMatch(s));

                        if (index < 0)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has a symbol ({symbol.Name}) that cannot be serialized.");
                        }

                        if (symbols.FindLastIndex(s => symbol.Name.IsMatch(s)) != index)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has an ambiguous symbol ({symbol.Name}).");
                        }

                        return Expression.SwitchCase(Codec.WriteInteger(Expression.Constant((long)index), context.Stream), Expression.Constant(symbol.Value));
                    });

                    result.Expression = Expression.Switch(value, cases.ToArray());
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="FixedSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is FixedSchema fixedSchema)
            {
                var expression = GenerateConversion(value, typeof(byte[]));

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                result.Expression = Expression.Block(
                    Expression.IfThen(
                        Expression.NotEqual(Expression.ArrayLength(expression), Expression.Constant(fixedSchema.Size)),
                        Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Only arrays of size {fixedSchema.Size} can be serialized to {fixedSchema.Name}.")))
                    ),
                    Codec.Write(expression, context.Stream));
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="FloatSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="float" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is FloatSchema)
            {
                result.Expression = Codec.WriteFloat(GenerateConversion(value, typeof(float)), context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is an <see cref="IntSchema" /> or a <see cref="LongSchema" />;
        /// an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="long" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is IntSchema || schema is LongSchema)
            {
                result.Expression = Codec.WriteInteger(GenerateConversion(value, typeof(long)), context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is a <see cref="MapResolution" /> and the schema
        /// is a <see cref="MapSchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not implement <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is MapSchema mapSchema)
            {
                if (resolution is MapResolution mapResolution)
                {
                    var keyVariable = Expression.Variable(mapResolution.KeyType);
                    var keySerializer = SerializerBuilder.BuildExpression(keyVariable, new StringSchema(), context);

                    var valueVariable = Expression.Variable(mapResolution.ValueType);
                    var valueSerializer = SerializerBuilder.BuildExpression(valueVariable, mapSchema.Value, context);

                    result.Expression = Codec.WriteMap(value, keyVariable, valueVariable, keySerializer, valueSerializer, context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="NullSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is NullSchema)
            {
                result.Expression = Expression.Empty();
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is a <see cref="RecordResolution" /> and the
        /// schema is a <see cref="RecordSchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not have a matching member for each field on the
        /// schema.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    var parameter = Expression.Parameter(typeof(Action<>).MakeGenericType(resolution.Type));
                    var reference = context.References.GetOrAdd((recordSchema, resolution.Type), parameter);
                    result.Expression = Expression.Invoke(reference, value);

                    if (parameter == reference)
                    {
                        var argument = Expression.Variable(resolution.Type);
                        var writes = recordSchema.Fields
                            .Select(field =>
                            {
                                var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));

                                if (match == null)
                                {
                                    throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.FullName} does not have a field or property that matches the {field.Name} field on {recordSchema.Name}.");
                                }

                                return SerializerBuilder.BuildExpression(Expression.PropertyOrField(argument, match.Member.Name), field.Type, context);
                            })
                            .ToList();

                        // .NET Framework doesn’t permit empty block expressions:
                        var expression = writes.Count > 0
                            ? Expression.Block(writes)
                            : Expression.Empty() as Expression;

                        expression = Expression.Lambda(expression, $"{recordSchema.Name} serializer", new[] { argument });

                        if (!context.Assignments.TryAdd(reference, expression))
                        {
                            throw new InvalidOperationException();
                        };
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="StringSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="string" />.
        /// </exception>
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
        {
            var result = new BinarySerializerBuildResult();

            if (schema is StringSchema)
            {
                var expression = GenerateConversion(value, typeof(string));

                var convertString = typeof(Encoding)
                    .GetMethod(nameof(Encoding.GetBytes), new[] { typeof(string) });

                expression = Expression.Call(Expression.Constant(Encoding.UTF8), convertString, expression);

                var bytes = Expression.Variable(expression.Type);

                expression = Expression.Block(
                    new[] { bytes },
                    Expression.Assign(bytes, expression),
                    Codec.WriteInteger(Expression.ConvertChecked(Expression.ArrayLength(bytes), typeof(long)), context.Stream),
                    Codec.Write(bytes, context.Stream)
                );

                result.Expression = expression;
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
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
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
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
                    result.Expression = Codec.WriteInteger(Expression.Divide(Expression.Subtract(Expression.Property(expression, utcTicks), epoch), factor), context.Stream);
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
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="resolution">
        /// The resolution to obtain type information from.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Information describing top-level expressions.
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
        public override IBinarySerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IBinarySerializerBuilderContext context)
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

                Expression writeIndex(Schema child) => Codec.WriteInteger(
                    Expression.Constant((long)schemas.IndexOf(child)),
                    context.Stream);

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
                            body = Expression.Block(
                                writeIndex(candidate),
                                SerializerBuilder.BuildExpression(Expression.ConvertChecked(value, underlying), candidate, context));
                        }
                        catch (Exception exception)
                        {
                            exceptions.Add(exception);
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
                            resolution.Type,
                            $"{resolution.Type.Name} does not match any non-null members of the union [{string.Join(", ", schemas.Select(s => s.GetType().Name))}].",
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
                            Expression.Constant($"Unexpected type encountered serializing to {resolution.Type.Name}.")));

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

                result.Expression = expression;
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
