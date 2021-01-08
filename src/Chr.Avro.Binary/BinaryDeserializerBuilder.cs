using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Xml;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// A function that deserializes a .NET object from serialized binary Avro.
    /// </summary>
    /// <param name="reader">
    /// The reader of the binary Avro data.
    /// </param>
    /// <returns>
    /// An binary Avro reader instance.
    /// </returns>
    public delegate T BinaryDeserializer<T>(ref BinaryReader reader);

    /// <summary>
    /// Builds Avro deserializers for .NET types.
    /// </summary>
    public interface IBinaryDeserializerBuilder
    {
        /// <summary>
        /// Builds a delegate that reads a serialized object.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        BinaryDeserializer<T> BuildDelegate<T>(Schema schema);

        /// <summary>
        /// Builds an expression that represents reading a serialized object.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be deserialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        Expression<BinaryDeserializer<T>> BuildExpression<T>(Schema schema);

        /// <summary>
        /// Builds an expression that represents reading an object of <paramref name="type" /> from
        /// a stream.
        /// </summary>
        /// <param name="type">
        /// The type of object to be deserialized.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Top-level expressions.
        /// </param>
        Expression BuildExpression(Type type, Schema schema, IBinaryDeserializerBuilderContext context);
    }

    /// <summary>
    /// Represents the outcome of a deserializer builder case.
    /// </summary>
    public interface IBinaryDeserializerBuildResult
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
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context);
    }

    /// <summary>
    /// An object that contains information to build a top-level deserialization function.
    /// </summary>
    public interface IBinaryDeserializerBuilderContext
    {
        /// <summary>
        /// A map of top-level variables to their values.
        /// </summary>
        ConcurrentDictionary<ParameterExpression, Expression> Assignments { get; }

        /// <summary>
        /// The input <see cref="BinaryReader" />.
        /// </summary>
        ParameterExpression Reader { get; }

        /// <summary>
        /// A map of schema-type pairs to top-level variables.
        /// </summary>
        ConcurrentDictionary<(Schema, Type), ParameterExpression> References { get; }
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
        /// <param name="resolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the deserializer
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public BinaryDeserializerBuilder(ITypeResolver? resolver = null)
            : this(DefaultCaseBuilders, resolver) { }

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
        /// Builds a delegate that reads a serialized object.
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
        public virtual BinaryDeserializer<T> BuildDelegate<T>(Schema schema)
        {
            return BuildExpression<T>(schema).Compile();
        }

        /// <summary>
        /// Builds an expression that represents reading a serialized object.
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
        public Expression<BinaryDeserializer<T>> BuildExpression<T>(Schema schema)
        {
            var context = new BinaryDeserializerBuilderContext();

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(typeof(T), schema, context);

            return Expression.Lambda<BinaryDeserializer<T>>(Expression.Block(
                context.Assignments
                    .Select(a => a.Key),
                context.Assignments
                    .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                    .Concat(new[] { root })
            ), new[] { context.Reader });
        }

        /// <summary>
        /// Builds an expression that represents reading an object of <paramref name="type" /> from
        /// a stream.
        /// </summary>
        /// <param name="type">
        /// The type of object to be deserialized.
        /// </param>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        /// <param name="context">
        /// Top-level expressions.
        /// </param>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the type to the schema.
        /// </exception>
        public virtual Expression BuildExpression(Type type, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var resolution = Resolver.ResolveType(type);
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.BuildExpression(resolution, schema, context);

                if (result.Expression != null)
                {
                    return result.Expression;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnsupportedTypeException(resolution.Type, $"No deserializer builder case matched {resolution.GetType().Name}.", new AggregateException(exceptions));
        }

        /// <summary>
        /// A default list of case builders.
        /// </summary>
        public static readonly IEnumerable<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> DefaultCaseBuilders =
            new Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>[]
            {
                // logical types:
                builder => new DecimalDeserializerBuilderCase(),
                builder => new DurationDeserializerBuilderCase(),
                builder => new TimestampDeserializerBuilderCase(),

                // primitives:
                builder => new BooleanDeserializerBuilderCase(),
                builder => new BytesDeserializerBuilderCase(),
                builder => new DoubleDeserializerBuilderCase(),
                builder => new FixedDeserializerBuilderCase(),
                builder => new FloatDeserializerBuilderCase(),
                builder => new IntegerDeserializerBuilderCase(),
                builder => new NullDeserializerBuilderCase(),
                builder => new StringDeserializerBuilderCase(),

                // collections:
                builder => new ArrayDeserializerBuilderCase(builder),
                builder => new MapDeserializerBuilderCase(builder),

                // enums:
                builder => new EnumDeserializerBuilderCase(),

                // records:
                builder => new RecordDeserializerBuilderCase(builder),

                // unions:
                builder => new UnionDeserializerBuilderCase(builder)
            };
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
        /// deserialized object. Since this is not a typed method, the general <see cref="Expression" />
        /// type is used.
        /// </remarks>
        public Expression? Expression { get; set; }

        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Expression" /> is
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
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        public abstract IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context);

        /// <summary>
        /// Generates a conversion from the intermediate type to the target type.
        /// </summary>
        /// <remarks>
        /// See the remarks for <see cref="Expression.ConvertChecked(Expression, Type)" />.
        /// </remarks>
        /// <param name="value">
        /// The value to convert.
        /// </param>
        /// <param name="target">
        /// The type to convert <paramref name="value" /> to.
        /// </param>
        protected virtual Expression GenerateConversion(Expression value, Type target)
        {
            if (value.Type == target)
            {
                return value;
            }

            try
            {
                return Expression.ConvertChecked(value, target);
            }
            catch (InvalidOperationException inner)
            {
                throw new UnsupportedTypeException(target, inner: inner);
            }
        }

        /// <summary>
        /// Generates an iteration to read items in an Avro block encoding.
        /// </summary>
        /// <param name="reader">
        /// A <see cref="BinaryReader" />.
        /// </param>
        /// <param name="body">
        /// The expression that will be executed for each item.
        /// </param>
        public virtual Expression GenerateIteration(Expression reader, Expression body)
        {
            var index = Expression.Variable(typeof(long));
            var size = Expression.Variable(typeof(long));

            var outer = Expression.Label();
            var inner = Expression.Label();

            var readInteger = typeof(BinaryReader)
                .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

            return Expression.Block(
                new[] { index, size },
                Expression.Loop(
                    Expression.Block(
                        Expression.Assign(
                            size,
                            Expression.Call(reader, readInteger)),
                        Expression.IfThen(
                            Expression.Equal(size, Expression.Constant(0L)),
                            Expression.Break(outer)),

                        // negative size indicates that the number of bytes in the block follows,
                        // so discard:
                        Expression.IfThen(
                            Expression.LessThan(size, Expression.Constant(0L)),
                            Expression.Block(
                                Expression.MultiplyAssign(size, Expression.Constant(-1L)),
                                Expression.Call(reader, readInteger))),

                        Expression.Assign(index, Expression.Constant(0L)),
                        Expression.Loop(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.Equal(Expression.PostIncrementAssign(index), size),
                                    Expression.Break(inner)),
                                body),
                            inner)),
                    outer));
        }
    }

    /// <summary>
    /// A base <see cref="IBinaryDeserializerBuilderContext" /> implementation.
    /// </summary>
    public class BinaryDeserializerBuilderContext : IBinaryDeserializerBuilderContext
    {
        /// <summary>
        /// A map of top-level variables to their values.
        /// </summary>
        public ConcurrentDictionary<ParameterExpression, Expression> Assignments { get; }

        /// <summary>
        /// The input <see cref="BinaryReader" />.
        /// </summary>
        public ParameterExpression Reader { get; }

        /// <summary>
        /// A map of types to top-level variables.
        /// </summary>
        public ConcurrentDictionary<(Schema, Type), ParameterExpression> References { get; }

        /// <summary>
        /// Creates a new context.
        /// </summary>
        /// <param name="reader">
        /// The input <see cref="BinaryReader" />. If an expression is not provided, one will
        /// be created.
        /// </param>
        public BinaryDeserializerBuilderContext(ParameterExpression? reader = null)
        {
            Assignments = new ConcurrentDictionary<ParameterExpression, Expression>();
            References = new ConcurrentDictionary<(Schema, Type), ParameterExpression>();
            Reader = reader ?? Expression.Parameter(typeof(BinaryReader).MakeByRefType());
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="ArraySchema" /> and attempts to map it
    /// to enumerable types.
    /// </summary>
    public class ArrayDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// The deserializer builder to use to build item deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new array deserializer builder case.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build item deserializers.
        /// </param>
        public ArrayDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
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
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is an <see cref="ArrayResolution" /> and the
        /// schema is an <see cref="ArraySchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type does not have an enumerable constructor and is not
        /// assignable from <see cref="List{T}" />.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var create = CreateIntermediateCollection(arrayResolution);

                    var readItem = DeserializerBuilder.BuildExpression(arrayResolution.ItemType, arraySchema.Item, context);
                    var collection = Expression.Parameter(create.Type);

                    var add = collection.Type.GetMethod("Add", new[] { readItem.Type });

                    Expression expression = Expression.Block(
                        new[] { collection },
                        Expression.Assign(collection, create),
                        GenerateIteration(context.Reader, Expression.Call(collection, add, readItem)),
                        collection
                    );

                    if (!arrayResolution.Type.IsAssignableFrom(expression.Type) && FindEnumerableConstructor(arrayResolution) is ConstructorResolution constructorResolution)
                    {
                        expression = Expression.New(constructorResolution.Constructor, expression);
                    }

                    result.Expression = GenerateConversion(expression, arrayResolution.Type);
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
        /// Creates an expression that represents instantiating a collection.
        /// </summary>
        protected virtual Expression CreateIntermediateCollection(ArrayResolution resolution)
        {
            if (resolution.Type.IsArray || resolution.Type.IsAssignableFrom(typeof(ArraySegment<>).MakeGenericType(resolution.ItemType)) || resolution.Type.IsAssignableFrom(typeof(ImmutableArray<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableArray)
                    .GetMethod(nameof(ImmutableArray.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableHashSet<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableHashSet)
                    .GetMethod(nameof(ImmutableHashSet.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableList<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableList)
                    .GetMethod(nameof(ImmutableList.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableSortedSet<>).MakeGenericType(resolution.ItemType)))
            {
                var createBuilder = typeof(ImmutableSortedSet)
                    .GetMethod(nameof(ImmutableSortedSet.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.ItemType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(HashSet<>).MakeGenericType(resolution.ItemType)))
            {
                return Expression.New(typeof(HashSet<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
            }

            if (resolution.Type.IsAssignableFrom(typeof(SortedSet<>).MakeGenericType(resolution.ItemType)))
            {
                return Expression.New(typeof(SortedSet<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
            }

            if (resolution.Type.IsAssignableFrom(typeof(Collection<>).MakeGenericType(resolution.ItemType)))
            {
                return Expression.New(typeof(Collection<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
            }

            return Expression.New(typeof(List<>).MakeGenericType(resolution.ItemType).GetConstructor(Type.EmptyTypes));
        }

        /// <summary>
        /// Attempts to find a constructor that takes a single enumerable parameter.
        /// </summary>
        protected virtual ConstructorResolution? FindEnumerableConstructor(ArrayResolution resolution)
        {
            return resolution.Constructors
                .Where(c => c.Parameters.Count == 1)
                .FirstOrDefault(c => c.Parameters.First().Type.IsAssignableFrom(typeof(IEnumerable<>).MakeGenericType(resolution.ItemType)));
        }

        /// <summary>
        /// Generates a conversion from the intermediate type to the target type.
        /// </summary>
        protected override Expression GenerateConversion(Expression value, Type target)
        {
            if (!value.Type.IsArray && (target.IsArray || target.IsAssignableFrom(typeof(ArraySegment<>).MakeGenericType(target.GenericTypeArguments))))
            {
                var toArray = value.Type
                    .GetMethod("ToArray", Type.EmptyTypes);

                value = Expression.Call(value, toArray);
            }
            else if (target.Assembly == typeof(ImmutableInterlocked).Assembly)
            {
                if (target.IsAssignableFrom(typeof(ImmutableQueue<>).MakeGenericType(target.GenericTypeArguments)))
                {
                    var createRange = typeof(ImmutableQueue)
                        .GetMethod(nameof(ImmutableQueue.CreateRange))
                        .MakeGenericMethod(target.GenericTypeArguments);

                    value = Expression.Call(null, createRange, value);
                }
                else if (target.IsAssignableFrom(typeof(ImmutableStack<>).MakeGenericType(target.GenericTypeArguments)))
                {
                    var createRange = typeof(ImmutableStack)
                        .GetMethod(nameof(ImmutableStack.CreateRange))
                        .MakeGenericMethod(target.GenericTypeArguments);

                    value = Expression.Call(null, createRange, value);
                }
                else
                {
                    var toImmutable = value.Type
                        .GetMethod("ToImmutable", Type.EmptyTypes);

                    value = Expression.Call(value, toImmutable);
                }
            }

            return base.GenerateConversion(value, target);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="BooleanSchema" /> and attempts to map
    /// it to any provided type.
    /// </summary>
    public class BooleanDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a boolean deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="bool" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is BooleanSchema)
            {
                var readBoolean = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadBoolean), Type.EmptyTypes);

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readBoolean), resolution.Type);
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
        /// Builds a variable-length bytes deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="T:System.Byte[]" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is BytesSchema)
            {
                var readBytes = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadBytes), Type.EmptyTypes);

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readBytes), resolution.Type);
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
        protected override Expression GenerateConversion(Expression value, Type target)
        {
            if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(guidConstructor, value);
            }

            return base.GenerateConversion(value, target);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="DecimalLogicalType" /> and attempts to
    /// map it to any provided type.
    /// </summary>
    public class DecimalDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a decimal deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="decimal" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;

                Expression expression;

                // figure out the size:
                if (schema is BytesSchema)
                {
                    var readBytes = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadBytes), Type.EmptyTypes);

                    expression = Expression.Call(context.Reader, readBytes);
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var readFixed = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadFixed), new[] { typeof(long) });

                    expression = Expression.Call(context.Reader, readFixed, Expression.Constant((long)fixedSchema.Size));
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                // declare variables for in-place transformation:
                var bytes = Expression.Variable(typeof(byte[]));
                var remainder = Expression.Variable(typeof(BigInteger));

                var divide = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.DivRem), new[] { typeof(BigInteger), typeof(BigInteger), typeof(BigInteger).MakeByRefType() });

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(byte[]) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                expression = Expression.Block(
                    new[] { bytes, remainder },

                    // store the bytes:
                    Expression.Assign(bytes, expression),

                    // BigInteger is little-endian, so reverse before creating:
                    Expression.Call(null, reverse, bytes),

                    Expression.Add(
                        Expression.ConvertChecked(
                            Expression.Call(
                                null,
                                divide,
                                Expression.New(integerConstructor, bytes),
                                Expression.Constant(BigInteger.Pow(10, scale)),
                                remainder),
                            typeof(decimal)),
                        Expression.Divide(
                            Expression.ConvertChecked(remainder, typeof(decimal)),
                            Expression.Constant((decimal)Math.Pow(10, scale))))
                );

                result.Expression = GenerateConversion(expression, resolution.Type);
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
        /// Builds a double deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="double" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is DoubleSchema)
            {
                var readDouble = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadDouble), Type.EmptyTypes);

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readDouble), resolution.Type);
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
        /// Builds a duration deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="TimeSpan" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
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

                    var readFixed = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadFixed), new[] { typeof(int) });

                    Expression read = Expression.Call(context.Reader, readFixed, Expression.Constant(4));

                    if (!BitConverter.IsLittleEndian)
                    {
                        var buffer = Expression.Variable(read.Type);
                        var reverse = typeof(Array)
                            .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                        read = Expression.Block(
                            new[] { buffer },
                            Expression.Assign(buffer, read),
                            Expression.Call(null, reverse, Expression.Convert(buffer, typeof(Array))),
                            buffer);
                    }

                    var toUInt32 = typeof(BitConverter)
                        .GetMethod(nameof(BitConverter.ToUInt32), new[] { typeof(byte[]), typeof(int) });

                    read = Expression.ConvertChecked(
                        Expression.Call(null, toUInt32, read, Expression.Constant(0)),
                        typeof(long));

                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string )});

                    var timeSpanConstructor = typeof(TimeSpan)
                        .GetConstructor(new[] { typeof(long) });

                    result.Expression = GenerateConversion(
                        Expression.Block(
                            Expression.IfThen(
                                Expression.NotEqual(read, Expression.Constant(0L)),
                                Expression.Throw(
                                    Expression.New(
                                        exceptionConstructor,
                                        Expression.Constant("Durations containing months cannot be accurately deserialized to a TimeSpan.")))),
                            Expression.New(
                                timeSpanConstructor,
                                Expression.AddChecked(
                                    Expression.MultiplyChecked(read, Expression.Constant(TimeSpan.TicksPerDay)),
                                    Expression.MultiplyChecked(read, Expression.Constant(TimeSpan.TicksPerMillisecond))))),
                        resolution.Type);
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
        /// Builds an enum deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when the the type does not contain a matching symbol for each symbol in the
        /// schema.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    Expression expression = Expression.ConvertChecked(Expression.Call(context.Reader, readInteger), typeof(int));

                    // find a match for each enum in the schema:
                    var cases = enumSchema.Symbols.Select((name, index) =>
                    {
                        var match = enumResolution.Symbols.SingleOrDefault(s => s.Name.IsMatch(name));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has no value that matches {name}.");
                        }

                        return Expression.SwitchCase(
                            GenerateConversion(Expression.Constant(match.Value), resolution.Type),
                            Expression.Constant(index)
                        );
                    });

                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    var exception = Expression.New(exceptionConstructor, Expression.Constant("Enum index out of range."));

                    // generate a switch on the index:
                    result.Expression = Expression.Switch(expression, Expression.Throw(exception, resolution.Type), cases.ToArray());
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
        /// Builds a fixed-length bytes deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="T:System.Byte[]" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is FixedSchema fixedSchema)
            {
                var readFixed = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadFixed), new[] { typeof(int) });

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readFixed, Expression.Constant(fixedSchema.Size)), resolution.Type);
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
        protected override Expression GenerateConversion(Expression value, Type target)
        {
            if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(guidConstructor, value);
            }

            return base.GenerateConversion(value, target);
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="FloatSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class FloatDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a float deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="float" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is FloatSchema)
            {
                var readSingle = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadSingle), Type.EmptyTypes);

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readSingle), resolution.Type);
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
        /// Builds an integer deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="long" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is IntSchema || schema is LongSchema)
            {
                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readInteger), resolution.Type);
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
        /// The deserializer builder to use to build key and value deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new map deserializer builder case.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build key and value deserializers.
        /// </param>
        public MapDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
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
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is a <see cref="MapResolution" /> and the schema
        /// is a <see cref="MapSchema" />; an unsuccessful result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type is not assignable from <see cref="Dictionary{TKey, TValue}" />.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is MapSchema mapSchema)
            {
                if (resolution is MapResolution mapResolution)
                {
                    var create = CreateIntermediateDictionary(mapResolution);

                    var readKey = DeserializerBuilder.BuildExpression(mapResolution.KeyType, new StringSchema(), context);
                    var readValue = DeserializerBuilder.BuildExpression(mapResolution.ValueType, mapSchema.Value, context);
                    var dictionary = Expression.Parameter(create.Type);

                    var add = dictionary.Type.GetMethod("Add", new[] { readKey.Type, readValue.Type });

                    Expression expression = Expression.Block(
                        new[] { dictionary },
                        Expression.Assign(dictionary, create),
                        GenerateIteration(context.Reader, Expression.Call(dictionary, add, readKey, readValue)),
                        dictionary
                    );

                    if (!mapResolution.Type.IsAssignableFrom(expression.Type) && FindDictionaryConstructor(mapResolution) is ConstructorResolution constructorResolution)
                    {
                        expression = Expression.New(constructorResolution.Constructor, new[] { expression });
                    }

                    result.Expression = GenerateConversion(expression, mapResolution.Type);
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
        /// Creates an expression that represents instantiating a dictionary.
        /// </summary>
        protected virtual Expression CreateIntermediateDictionary(MapResolution resolution)
        {
            if (resolution.Type.IsAssignableFrom(typeof(ImmutableDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                var createBuilder = typeof(ImmutableDictionary)
                    .GetMethod(nameof(ImmutableDictionary.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.KeyType, resolution.ValueType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(ImmutableSortedDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                var createBuilder = typeof(ImmutableSortedDictionary)
                    .GetMethod(nameof(ImmutableSortedDictionary.CreateBuilder), Type.EmptyTypes)
                    .MakeGenericMethod(resolution.KeyType, resolution.ValueType);

                return Expression.Call(null, createBuilder);
            }

            if (resolution.Type.IsAssignableFrom(typeof(SortedDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                return Expression.New(typeof(SortedDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType).GetConstructor(Type.EmptyTypes));
            }

            if (resolution.Type.IsAssignableFrom(typeof(SortedList<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)))
            {
                return Expression.New(typeof(SortedList<,>).MakeGenericType(resolution.KeyType, resolution.ValueType).GetConstructor(Type.EmptyTypes));
            }

            return Expression.New(typeof(Dictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType).GetConstructor(Type.EmptyTypes));
        }

        /// <summary>
        /// Attempts to find a constructor that takes a single dictionary parameter.
        /// </summary>
        protected virtual ConstructorResolution? FindDictionaryConstructor(MapResolution resolution)
        {
            return resolution.Constructors
                .Where(c => c.Parameters.Count == 1)
                .FirstOrDefault(c => c.Parameters.First().Type.IsAssignableFrom(typeof(IDictionary<,>).MakeGenericType(resolution.KeyType, resolution.ValueType)));
        }

        /// <summary>
        /// Generates a conversion from the intermediate type to the target type.
        /// </summary>
        protected override Expression GenerateConversion(Expression value, Type target)
        {
            if (target.Assembly == typeof(ImmutableInterlocked).Assembly)
            {
                var toImmutable = value.Type
                    .GetMethod("ToImmutable", Type.EmptyTypes);

                value = Expression.Call(value, toImmutable);
            }

            return base.GenerateConversion(value, target);
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
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the schema is a <see cref="NullSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is NullSchema)
            {
                result.Expression = Expression.Default(resolution.Type);
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
        /// <param name="context">
        /// Information describing top-level expressions.
        /// </param>
        /// <returns>
        /// A successful result if the resolution is a <see cref="RecordResolution" /> and the
        /// schema is a <see cref="RecordSchema" />; an unsuccessful result otherwise.
        /// </returns>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    // since record deserialization is potentially recursive, create a delegate and
                    // return its invocation:
                    var parameter = Expression.Parameter(Expression.GetDelegateType(context.Reader.Type.MakeByRefType(), resolution.Type));
                    var reference = context.References.GetOrAdd((recordSchema, resolution.Type), parameter);
                    result.Expression = Expression.Invoke(reference, context.Reader);

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        Expression expression;

                        if (FindRecordConstructor(recordResolution, recordSchema) is ConstructorResolution constructorResolution)
                        {
                            // map constructor parameters to fields:
                            var mapping = recordSchema.Fields
                                .Select(field =>
                                {
                                    // there will be a match or we wouldn’t have made it this far:
                                    var match = constructorResolution.Parameters.Single(f => f.Name.IsMatch(field.Name));
                                    var parameter = Expression.Parameter(match.Type);

                                    return (
                                        Match: match,
                                        Parameter: parameter,
                                        Assignment: (Expression)Expression.Assign(
                                            parameter,
                                            DeserializerBuilder.BuildExpression(match.Type, field.Type, context))
                                    );
                                })
                                .ToDictionary(r => r.Match, r => (r.Parameter, r.Assignment));

                            expression = Expression.Block(
                                mapping
                                    .Select(d => d.Value.Parameter),
                                mapping
                                    .Select(d => d.Value.Assignment)
                                    .Concat(new[]
                                    {
                                        Expression.New(
                                            constructorResolution.Constructor,
                                            constructorResolution.Parameters
                                                .Select(parameter => mapping.ContainsKey(parameter)
                                                    ? (Expression)mapping[parameter].Parameter
                                                    : Expression.Constant(parameter.Parameter.DefaultValue)))
                                    }));
                        }
                        else
                        {
                            var value = Expression.Parameter(resolution.Type);

                            expression = Expression.Block(
                                new[] { value },
                                new[] { (Expression)Expression.Assign(value, Expression.New(value.Type)) }
                                    .Concat(recordSchema.Fields.Select(field =>
                                    {
                                        var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));
                                        var schema = match == null ? CreateSurrogateSchema(field.Type) : field.Type;
                                        var type = match == null ? CreateSurrogateType(schema) : match.Type;

                                        // always read to advance the stream:
                                        var expression = DeserializerBuilder.BuildExpression(type, schema, context);

                                        if (match != null)
                                        {
                                            // and assign if a field matches:
                                            expression = Expression.Assign(Expression.PropertyOrField(value, match.Member.Name), expression);
                                        }

                                        return expression;
                                    }))
                                    .Concat(new[] { value })
                            );
                        }

                        expression = Expression.Lambda(parameter.Type, expression, $"{recordSchema.Name} deserializer", new[] { context.Reader });

                        if (!context.Assignments.TryAdd(reference, expression))
                        {
                            throw new InvalidOperationException();
                        }
                    }
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
        /// Creates a schema that can be used to deserialize missing record fields.
        /// </summary>
        /// <param name="schema">
        /// The schema to alter.
        /// </param>
        /// <returns>
        /// A schema that can be mapped to a surrogate type.
        /// </returns>
        protected virtual Schema CreateSurrogateSchema(Schema schema)
        {
            return schema switch
            {
                ArraySchema array => new ArraySchema(CreateSurrogateSchema(array.Item)),
                EnumSchema _ => new LongSchema(),
                MapSchema map => new MapSchema(CreateSurrogateSchema(map.Value)),
                UnionSchema union => new UnionSchema(union.Schemas.Select(CreateSurrogateSchema).ToList()),
                _ => schema
            };
        }

        /// <summary>
        /// Creates a type that can be used to deserialize missing record fields.
        /// </summary>
        /// <param name="schema">
        /// The schema to select a compatible type for.
        /// </param>
        /// <returns>
        /// <see cref="IEnumerable{T}" /> if the schema is an array schema (or a union schema
        /// containing only array/null schemas), <see cref="IDictionary{TKey, TValue}" /> if the
        /// schema is a map schema (or a union schema containing only map/null schemas), and
        /// <see cref="Object" /> otherwise.
        /// </returns>
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

        /// <summary>
        /// Attempts to find a constructor with a matching parameter for each of a record schema’s
        /// fields.
        /// </summary>
        protected ConstructorResolution? FindRecordConstructor(RecordResolution resolution, RecordSchema schema)
        {
            return resolution.Constructors.FirstOrDefault(constructor =>
                schema.Fields.All(field =>
                    constructor.Parameters.Any(parameter =>
                        parameter.Name.IsMatch(field.Name))));
        }
    }

    /// <summary>
    /// A deserializer builder case that matches <see cref="StringSchema" /> and attempts to map it
    /// to any provided type.
    /// </summary>
    public class StringDeserializerBuilderCase : BinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a string deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="string" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is StringSchema)
            {
                var readString = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadString), Type.EmptyTypes);

                result.Expression = GenerateConversion(Expression.Call(context.Reader, readString), resolution.Type);
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
        protected override Expression GenerateConversion(Expression value, Type target)
        {
            if (target == typeof(DateTime) || target == typeof(DateTime?))
            {
                var parseDateTime = typeof(DateTime)
                    .GetMethod(nameof(DateTime.Parse), new[]
                    {
                        value.Type,
                        typeof(IFormatProvider),
                        typeof(DateTimeStyles)
                    });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateTime,
                        value,
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
                        value.Type,
                        typeof(IFormatProvider),
                        typeof(DateTimeStyles)
                    });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateTimeOffset,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture),
                        Expression.Constant(DateTimeStyles.RoundtripKind)
                    ),
                    target
                );
            }
            else if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(guidConstructor, value);
            }
            else if (target == typeof(TimeSpan) || target == typeof(TimeSpan?))
            {
                var parseTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToTimeSpan));

                value = Expression.Call(null, parseTimeSpan, value);
            }
            else if (target == typeof(Uri))
            {
                var uriConstructor = typeof(Uri)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(uriConstructor, value);
            }

            return base.GenerateConversion(value, target);
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
        /// Builds a timestamp deserializer for a type-schema pair.
        /// </summary>
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
        /// Thrown when <see cref="DateTime" /> cannot be converted to the resolved type.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
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

                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    Expression expression = Expression.Call(context.Reader, readInteger);

                    var addTicks = typeof(DateTime)
                        .GetMethod(nameof(DateTime.AddTicks));

                    // result = epoch.AddTicks(value * factor);
                    result.Expression = GenerateConversion(
                        Expression.Call(epoch, addTicks, Expression.Multiply(expression, factor)),
                        resolution.Type
                    );
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
        /// The deserializer builder to use to build child deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Creates a new record deserializer builder case.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// The deserializer builder to use to build child deserializers.
        /// </param>
        public UnionDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
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
        /// Thrown when the type cannot be mapped to each schema in the union.
        /// </exception>
        public override IBinaryDeserializerBuildResult BuildExpression(TypeResolution resolution, Schema schema, IBinaryDeserializerBuilderContext context)
        {
            var result = new BinaryDeserializerBuildResult();

            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                Expression expression = Expression.Call(context.Reader, readInteger);

                // create a mapping for each schema in the union:
                var cases = unionSchema.Schemas.Select((child, index) =>
                {
                    var selected = SelectType(resolution, child);
                    var underlying = Nullable.GetUnderlyingType(selected.Type);

                    if (child is NullSchema && selected.Type.IsValueType && underlying == null)
                    {
                        throw new UnsupportedTypeException(resolution.Type, $"A deserializer for a union containing {typeof(NullSchema)} cannot be built for {selected.Type.FullName}.");
                    }

                    underlying = selected.Type;

                    var @case = DeserializerBuilder.BuildExpression(underlying, child, context);

                    return Expression.SwitchCase(
                        GenerateConversion(@case, resolution.Type),
                        Expression.Constant((long)index)
                    );
                });

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                var exception = Expression.New(exceptionConstructor, Expression.Constant("Union index out of range."));

                // generate a switch on the index:
                result.Expression = Expression.Switch(expression, Expression.Throw(exception, resolution.Type), cases.ToArray());
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
