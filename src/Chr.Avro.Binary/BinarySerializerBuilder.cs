using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Xml;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// A function that serializes a .NET object to binary Avro.
    /// </summary>
    /// <param name="value">
    /// An unserialized value.
    /// </param>
    /// <param name="writer">
    /// A binary Avro writer instance.
    /// </param>
    public delegate void BinarySerializer<T>(T value, BinaryWriter writer);

    /// <summary>
    /// Builds Avro serializers for .NET types.
    /// </summary>
    public interface IBinarySerializerBuilder
    {
        /// <summary>
        /// Builds a delegate that writes a serialized object.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        BinarySerializer<T> BuildDelegate<T>(Schema schema);

        /// <summary>
        /// Builds an expression that represents writing a serialized object.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        Expression<BinarySerializer<T>> BuildExpression<T>(Schema schema);

        /// <summary>
        /// Builds an expression that represents writing <paramref name="value" /> to a span.
        /// </summary>
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="schema">
        /// The schema to map to <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// Top-level expressions.
        /// </param>
        Expression BuildExpression(Expression value, Schema schema, IBinarySerializerBuilderContext context);
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
        /// The output <see cref="BinaryWriter" />.
        /// </summary>
        ParameterExpression Writer { get; }
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
        /// <param name="resolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the serializer
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public BinarySerializerBuilder(ITypeResolver? resolver = null)
            : this(DefaultCaseBuilders, resolver) { }

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
        /// Builds a delegate that writes a serialized object.
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
        public virtual BinarySerializer<T> BuildDelegate<T>(Schema schema)
        {
            return BuildExpression<T>(schema).Compile();
        }

        /// <summary>
        /// Builds an expression that represents writing a serialized object.
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
        public virtual Expression<BinarySerializer<T>> BuildExpression<T>(Schema schema)
        {
            var context = new BinarySerializerBuilderContext();
            var value = Expression.Parameter(typeof(T));

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(value, schema, context);

            return Expression.Lambda<BinarySerializer<T>>(Expression.Block(
                context.Assignments.Keys,
                context.Assignments
                    .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                    .Concat(new[] { root })
            ), new[] { value, context.Writer });
        }

        /// <summary>
        /// Builds an expression that represents writing <paramref name="value" /> to a span.
        /// </summary>
        /// <param name="value">
        /// An expression that represents the value to be serialized.
        /// </param>
        /// <param name="schema">
        /// The schema to map to <paramref name="value" />.
        /// </param>
        /// <param name="context">
        /// Top-level expressions.
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
        /// A default list of case builders.
        /// </summary>
        public static readonly IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> DefaultCaseBuilders =
            new Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>[]
            {
                // logical types:
                builder => new DecimalSerializerBuilderCase(),
                builder => new DurationSerializerBuilderCase(),
                builder => new TimestampSerializerBuilderCase(),

                // primitives:
                builder => new BooleanSerializerBuilderCase(),
                builder => new BytesSerializerBuilderCase(),
                builder => new DoubleSerializerBuilderCase(),
                builder => new FixedSerializerBuilderCase(),
                builder => new FloatSerializerBuilderCase(),
                builder => new IntegerSerializerBuilderCase(),
                builder => new NullSerializerBuilderCase(),
                builder => new StringSerializerBuilderCase(),

                // collections:
                builder => new ArraySerializerBuilderCase(builder),
                builder => new MapSerializerBuilderCase(builder),

                // enums:
                builder => new EnumSerializerBuilderCase(),

                // records:
                builder => new RecordSerializerBuilderCase(builder),

                // unions:
                builder => new UnionSerializerBuilderCase(builder)
            };
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
        /// <param name="value">
        /// The value to convert.
        /// </param>
        /// <param name="intermediate">
        /// The type to convert <paramref name="value" /> to.
        /// </param>
        protected virtual Expression GenerateConversion(Expression value, Type intermediate)
        {
            if (value.Type == intermediate)
            {
                return value;
            }

            try
            {
                return Expression.ConvertChecked(value, intermediate);
            }
            catch (InvalidOperationException inner)
            {
                throw new UnsupportedTypeException(intermediate, $"Failed to generate a conversion for {value.Type}.", inner);
            }
        }

        /// <summary>
        /// Generates an iteration to write items in an Avro block encoding.
        /// </summary>
        /// <param name="writer">
        /// A <see cref="BinaryWriter" />.
        /// </param>
        /// <param name="items">
        /// An <see cref="IEnumerable{T}" />.
        /// </param>
        /// <param name="item">
        /// A variable that will be assigned an item prior to invoking <paramref name="body" />.
        /// </param>
        /// <param name="body">
        /// The expression that will be executed for each item.
        /// </param>
        public Expression GenerateIteration(Expression writer, Expression items, ParameterExpression item, Expression body)
        {
            var collection = Expression.Variable(typeof(ICollection<>).MakeGenericType(item.Type));
            var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(item.Type));
            var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(item.Type));

            var loop = Expression.Label();

            var dispose = typeof(IDisposable)
                .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

            var getCount = collection.Type
                .GetProperty("Count")
                .GetGetMethod();

            var getCurrent = enumerator.Type
                .GetProperty("Current")
                .GetGetMethod();

            var getEnumerator = typeof(IEnumerable<>)
                .MakeGenericType(item.Type)
                .GetMethod("GetEnumerator", Type.EmptyTypes);

            var moveNext = typeof(IEnumerator)
                .GetMethod("MoveNext", Type.EmptyTypes);

            var toList = typeof(Enumerable)
                .GetMethod(nameof(Enumerable.ToList))
                .MakeGenericMethod(item.Type);

            var writeInteger = typeof(BinaryWriter)
                .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

            return Expression.Block(
                new[] { enumerator, collection },
                Expression.Assign(
                    collection,
                    Expression.Condition(
                        Expression.TypeIs(items, collection.Type),
                        Expression.Convert(items, collection.Type),
                        Expression.Convert(Expression.Call(null, toList, Expression.Convert(items, enumerable.Type)), collection.Type))),
                Expression.IfThen(
                    Expression.GreaterThan(Expression.Property(collection, getCount), Expression.Constant(0)),
                    Expression.Block(
                        Expression.Call(writer, writeInteger, Expression.Convert(Expression.Property(collection, getCount), typeof(long))),
                        Expression.Assign(enumerator, Expression.Call(collection, getEnumerator)),
                        Expression.TryFinally(
                            Expression.Loop(
                                Expression.IfThenElse(
                                    Expression.Call(enumerator, moveNext),
                                    Expression.Block(
                                        new[] { item },
                                        Expression.Assign(item, Expression.Property(enumerator, getCurrent)),
                                        body),
                                    Expression.Break(loop)),
                                loop),
                            Expression.Call(enumerator, dispose)))),
                Expression.Call(writer, writeInteger, Expression.Constant(0L)));
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
        /// The output <see cref="BinaryWriter" />.
        /// </summary>
        public ParameterExpression Writer { get; }

        /// <summary>
        /// Creates a new context.
        /// </summary>
        /// <param name="writer">
        /// The output <see cref="BinaryWriter" />. If an expression is not provided, one will
        /// be created.
        /// </param>
        public BinarySerializerBuilderContext(ParameterExpression? writer = null)
        {
            Assignments = new ConcurrentDictionary<ParameterExpression, Expression>();
            References = new ConcurrentDictionary<(Schema, Type), ParameterExpression>();
            Writer = writer ?? Expression.Parameter(typeof(BinaryWriter));
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="ArraySchema" /> and attempts to map it to
    /// enumerable types.
    /// </summary>
    public class ArraySerializerBuilderCase : BinarySerializerBuilderCase
    {
        /// <summary>
        /// The serializer builder to use to build item serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new array serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build item serializers.
        /// </param>
        public ArraySerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
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
                    var item = Expression.Variable(arrayResolution.ItemType);
                    var itemSerializer = SerializerBuilder.BuildExpression(item, arraySchema.Item, context);

                    result.Expression = GenerateIteration(context.Writer, value, item, itemSerializer);
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
                var writeBoolean = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteBoolean), new[] { typeof(bool) });

                result.Expression = Expression.Call(context.Writer, writeBoolean, GenerateConversion(value, typeof(bool)));
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
                var writeBytes = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteBytes), new[] { typeof(byte[]) });

                result.Expression = Expression.Call(context.Writer, writeBytes, GenerateConversion(value, typeof(byte[])));
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
        protected override Expression GenerateConversion(Expression value, Type intermediate)
        {
            if (value.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                value = Expression.Call(value, convertGuid);
            }

            return base.GenerateConversion(value, intermediate);
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="DecimalLogicalType" /> and attempts to
    /// map it to any provided type.
    /// </summary>
    public class DecimalSerializerBuilderCase : BinarySerializerBuilderCase
    {
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
                    var writeBytes = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteBytes), new[] { typeof(byte[]) });

                    expression = Expression.Block(
                        new[] { bytes },
                        expression,
                        Expression.Call(context.Writer, writeBytes, bytes)
                    );
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    var writeFixed = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteFixed), new[] { typeof(byte[]) });

                    expression = Expression.Block(
                        new[] { bytes },
                        expression,
                        Expression.IfThen(
                            Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema.Name} (size {fixedSchema.Size}) and decimal with precision {precision} and scale {scale}.")))
                        ),
                        Expression.Call(context.Writer, writeFixed, bytes)
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
                var writeDouble = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteDouble), new[] { typeof(double) });

                result.Expression = Expression.Call(context.Writer, writeDouble, GenerateConversion(value, typeof(double)));
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
                if (resolution is DurationResolution)
                {
                    if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                    {
                        throw new UnsupportedSchemaException(schema);
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

                        var writeFixed = typeof(BinaryWriter)
                            .GetMethod(nameof(BinaryWriter.WriteFixed), new[] { bytes.Type });

                        return Expression.Call(context.Writer, writeFixed, bytes);
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
    /// A serializer builder case that matches <see cref="EnumSchema" /> and attempts to map it to
    /// enum types.
    /// </summary>
    public class EnumSerializerBuilderCase : BinarySerializerBuilderCase
    {
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
                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

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

                        return Expression.SwitchCase(
                            Expression.Call(context.Writer, writeInteger, Expression.Constant((long)index)),
                            Expression.Constant(symbol.Value));
                    });

                    var exceptionConstructor = typeof(ArgumentOutOfRangeException)
                        .GetConstructor(new[] { typeof(string) });

                    var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum value out of range."));

                    result.Expression = Expression.Switch(value, Expression.Throw(exception), cases.ToArray());
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

                var writeFixed = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteFixed), new[] { typeof(byte[]) });

                result.Expression = Expression.Block(
                    Expression.IfThen(
                        Expression.NotEqual(Expression.ArrayLength(expression), Expression.Constant(fixedSchema.Size)),
                        Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Only arrays of size {fixedSchema.Size} can be serialized to {fixedSchema.Name}.")))
                    ),
                    Expression.Call(context.Writer, writeFixed, expression));
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
                var writeSingle = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteSingle), new[] { typeof(float) });

                result.Expression = Expression.Call(context.Writer, writeSingle, GenerateConversion(value, typeof(float)));
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
                var writeInteger = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                result.Expression = Expression.Call(context.Writer, writeInteger, GenerateConversion(value, typeof(long)));
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
        /// The serializer builder to use to build key and value serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new map serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build key and value serializers.
        /// </param>
        public MapSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
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
                    var pair = Expression.Variable(typeof(KeyValuePair<,>).MakeGenericType(mapResolution.KeyType, mapResolution.ValueType));

                    var getKey = pair.Type
                        .GetProperty("Key")
                        .GetGetMethod();

                    var getValue = pair.Type
                        .GetProperty("Value")
                        .GetGetMethod();

                    var keySerializer = SerializerBuilder.BuildExpression(Expression.Property(pair, getKey), new StringSchema(), context);
                    var valueSerializer = SerializerBuilder.BuildExpression(Expression.Property(pair, getValue), mapSchema.Value, context);

                    result.Expression = GenerateIteration(context.Writer, value, pair, Expression.Block(keySerializer, valueSerializer));
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
                    // since record serialization is potentially recursive, create a delegate and
                    // return its invocation:
                    var parameter = Expression.Parameter(Expression.GetDelegateType(resolution.Type, context.Writer.Type, typeof(void)));
                    var reference = context.References.GetOrAdd((recordSchema, resolution.Type), parameter);
                    result.Expression = Expression.Invoke(reference, value, context.Writer);

                    // then build/set the delegate if it hasn’t been built yet:
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

                        expression = Expression.Lambda(parameter.Type, expression, $"{recordSchema.Name} serializer", new[] { argument, context.Writer });

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
                var writeString = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteString), new[] { typeof(string) });

                result.Expression = Expression.Call(context.Writer, writeString, GenerateConversion(value, typeof(string)));
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

                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                    result.Expression = Expression.Call(
                        context.Writer,
                        writeInteger,

                        // (value.UtcTicks - epoch) / factor
                        Expression.Divide(
                            Expression.Subtract(Expression.Property(expression, utcTicks), epoch),
                            factor));
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
        /// The serializer builder to use to build child serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new union serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build child serializers.
        /// </param>
        public UnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
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

                var writeInteger = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                Expression writeIndex(Schema child) => Expression.Call(
                    context.Writer,
                    writeInteger,
                    Expression.Constant((long)schemas.IndexOf(child)));

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
                                SerializerBuilder.BuildExpression(Expression.Convert(value, underlying), candidate, context));
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
