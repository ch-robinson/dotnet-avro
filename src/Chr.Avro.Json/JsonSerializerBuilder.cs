using Chr.Avro.Abstract;
using Chr.Avro.Resolution;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Numerics;
using System.Reflection;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Xml;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// A function that serializes a .NET object to a JSON-encoded Avro value.
    /// </summary>
    /// <param name="value">
    /// The unserialized value.
    /// </param>
    /// <param name="writer">
    /// A writer around the output stream.
    /// </param>
    public delegate void JsonSerializer<T>(T value, Utf8JsonWriter writer);

    /// <summary>
    /// Builds Avro serializers for .NET types.
    /// </summary>
    public interface IJsonSerializerBuilder
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
        JsonSerializer<T> BuildDelegate<T>(Schema schema);

        /// <summary>
        /// Builds an expression that represents writing a serialized object.
        /// </summary>
        /// <typeparam name="T">
        /// The type of object to be serialized.
        /// </typeparam>
        /// <param name="schema">
        /// The schema to map to the type.
        /// </param>
        Expression<JsonSerializer<T>> BuildExpression<T>(Schema schema);

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
        Expression BuildExpression(Expression value, Schema schema, IJsonSerializerBuilderContext context);
    }

    /// <summary>
    /// Represents the outcome of a serializer builder case.
    /// </summary>
    public interface IJsonSerializerBuildResult
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
    /// <see cref="JsonSerializerBuilder" /> to break apart serializer building logic.
    /// </summary>
    public interface IJsonSerializerBuilderCase
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
        IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context);
    }

    /// <summary>
    /// An object that contains information to build a top-level serialization function.
    /// </summary>
    public interface IJsonSerializerBuilderContext
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
        /// The output <see cref="Utf8JsonWriter" />.
        /// </summary>
        ParameterExpression Writer { get; }
    }

    /// <summary>
    /// A serializer builder configured with a reasonable set of default cases.
    /// </summary>
    public class JsonSerializerBuilder : IJsonSerializerBuilder
    {
        /// <summary>
        /// A list of cases that the build methods will attempt to apply. If the first case does
        /// not match, the next case will be tested, and so on.
        /// </summary>
        public IEnumerable<IJsonSerializerBuilderCase> Cases { get; }

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
        public JsonSerializerBuilder(ITypeResolver? resolver = null)
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
        public JsonSerializerBuilder(IEnumerable<Func<IJsonSerializerBuilder, IJsonSerializerBuilderCase>> caseBuilders, ITypeResolver? resolver = null)
        {
            var cases = new List<IJsonSerializerBuilderCase>();

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
        public virtual JsonSerializer<T> BuildDelegate<T>(Schema schema)
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
        public virtual Expression<JsonSerializer<T>> BuildExpression<T>(Schema schema)
        {
            var context = new JsonSerializerBuilderContext();
            var value = Expression.Parameter(typeof(T));

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(value, schema, context);

            var flush = typeof(Utf8JsonWriter)
                .GetMethod(nameof(Utf8JsonWriter.Flush), Type.EmptyTypes);

            return Expression.Lambda<JsonSerializer<T>>(Expression.Block(
                context.Assignments.Keys,
                context.Assignments
                    .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                    .Concat(new[]
                    {
                        root,
                        Expression.Call(context.Writer, flush)
                    })
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
        public virtual Expression BuildExpression(Expression value, Schema schema, IJsonSerializerBuilderContext context)
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
        public static readonly IEnumerable<Func<IJsonSerializerBuilder, IJsonSerializerBuilderCase>> DefaultCaseBuilders =
            new Func<IJsonSerializerBuilder, IJsonSerializerBuilderCase>[]
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
                builder => new IntSerializerBuilderCase(),
                builder => new LongSerializerBuilderCase(),
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
    /// A base <see cref="IJsonSerializerBuildResult" /> implementation.
    /// </summary>
    public class JsonSerializerBuildResult : IJsonSerializerBuildResult
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
    /// A base <see cref="IJsonSerializerBuilderCase" /> implementation.
    /// </summary>
    public abstract class JsonSerializerBuilderCase : IJsonSerializerBuilderCase
    {
        /// <summary>
        /// An encoder that can be used to write char arrays encoded as JSON strings. This encoder
        /// is configured to write each char as a Unicode escape sequence.
        /// </summary>
        protected static readonly JavaScriptEncoder ByteEncoder = JavaScriptEncoder.Create();

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
        public abstract IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context);

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
        /// Generates an iteration to write items in an array or object.
        /// </summary>
        /// <param name="items">
        /// An <see cref="IEnumerable{T}" />.
        /// </param>
        /// <param name="item">
        /// A variable that will be assigned an item prior to invoking <paramref name="body" />.
        /// </param>
        /// <param name="body">
        /// The expression that will be executed for each item.
        /// </param>
        public Expression GenerateIteration(Expression items, ParameterExpression item, Expression body)
        {
            var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(item.Type));
            var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(item.Type));

            var loop = Expression.Label();

            var dispose = typeof(IDisposable)
                .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

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

            return Expression.Block(
                new[] { enumerator },
                Expression.Block(
                    Expression.Assign(enumerator, Expression.Call(items, getEnumerator)),
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
                        Expression.Call(enumerator, dispose))));
        }
    }

    /// <summary>
    /// A base <see cref="IJsonSerializerBuilderContext" /> implementation.
    /// </summary>
    public class JsonSerializerBuilderContext : IJsonSerializerBuilderContext
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
        /// The output <see cref="Utf8JsonWriter" />.
        /// </summary>
        public ParameterExpression Writer { get; }

        /// <summary>
        /// Creates a new context.
        /// </summary>
        /// <param name="writer">
        /// The output <see cref="Utf8JsonWriter" />. If an expression is not provided, one will
        /// be created.
        /// </param>
        public JsonSerializerBuilderContext(ParameterExpression? writer = null)
        {
            Assignments = new ConcurrentDictionary<ParameterExpression, Expression>();
            References = new ConcurrentDictionary<(Schema, Type), ParameterExpression>();
            Writer = writer ?? Expression.Parameter(typeof(Utf8JsonWriter));
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="ArraySchema" /> and attempts to map it to
    /// enumerable types.
    /// </summary>
    public class ArraySerializerBuilderCase : JsonSerializerBuilderCase
    {
        /// <summary>
        /// The serializer builder to use to build item serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new array serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build item serializers.
        /// </param>
        public ArraySerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var item = Expression.Variable(arrayResolution.ItemType);
                    var itemSerializer = SerializerBuilder.BuildExpression(item, arraySchema.Item, context);

                    var writeStartArray = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartArray), Type.EmptyTypes);

                    var writeEndArray = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndArray), Type.EmptyTypes);

                    result.Expression = Expression.Block(
                        Expression.Call(context.Writer, writeStartArray),
                        GenerateIteration(value, item, itemSerializer),
                        Expression.Call(context.Writer, writeEndArray));
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
    public class BooleanSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is BooleanSchema)
            {
                var writeBoolean = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteBooleanValue), new[] { typeof(bool) });

                result.Expression = Expression.Call(
                    context.Writer,
                    writeBoolean,
                    GenerateConversion(value, typeof(bool)));
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
    public class BytesSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is BytesSchema)
            {
                var bytes = Expression.Parameter(typeof(byte[]));
                var chars = Expression.Parameter(typeof(char[]));

                var copyTo = typeof(Array)
                    .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

                var encode = typeof(JsonEncodedText)
                    .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                result.Expression = Expression.Block(
                    new[] { bytes, chars },
                    Expression.Assign(bytes, GenerateConversion(value, typeof(byte[]))),
                    Expression.Assign(chars, Expression.NewArrayBounds(typeof(char), Expression.ArrayLength(bytes))),
                    Expression.Call(bytes, copyTo, chars, Expression.Constant(0)),
                    Expression.Call(
                        context.Writer,
                        writeString,
                        Expression.Call(
                            null,
                            encode,
                            Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                            Expression.Constant(ByteEncoder))));
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
    public class DecimalSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = decimalLogicalType.Precision;
                var scale = decimalLogicalType.Scale;

                var expression = GenerateConversion(value, typeof(decimal));

                // declare variables for in-place transformation:
                var bytes = Expression.Variable(typeof(byte[]));
                var chars = Expression.Parameter(typeof(char[]));

                var integerConstructor = typeof(BigInteger)
                    .GetConstructor(new[] { typeof(decimal) });

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { typeof(Array) });

                var toByteArray = typeof(BigInteger)
                    .GetMethod(nameof(BigInteger.ToByteArray), Type.EmptyTypes);

                var copyTo = typeof(Array)
                    .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

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

                    Expression.Assign(chars, Expression.NewArrayBounds(typeof(char), Expression.ArrayLength(bytes))),
                    Expression.Call(bytes, copyTo, chars, Expression.Constant(0))
                );

                var encode = typeof(JsonEncodedText)
                    .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                // figure out how to write:
                if (schema is BytesSchema)
                {
                    expression = Expression.Block(
                        new[] { bytes, chars },
                        expression,
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                null,
                                encode,
                                Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                Expression.Constant(ByteEncoder))));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var exceptionConstructor = typeof(OverflowException)
                        .GetConstructor(new[] { typeof(string) });

                    expression = Expression.Block(
                        new[] { bytes, chars },
                        expression,
                        Expression.IfThen(
                            Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Size mismatch between {fixedSchema.Name} (size {fixedSchema.Size}) and decimal with precision {precision} and scale {scale}.")))),
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                null,
                                encode,
                                Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                Expression.Constant(ByteEncoder))));
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
    public class DoubleSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is DoubleSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(double) });

                result.Expression = Expression.Call(
                    context.Writer,
                    writeNumber,
                    GenerateConversion(value, typeof(double)));
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
    public class DurationSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema.LogicalType is DurationLogicalType)
            {
                if (resolution is DurationResolution)
                {
                    if (!(schema is FixedSchema fixedSchema && fixedSchema.Size == 12))
                    {
                        throw new UnsupportedSchemaException(schema);
                    }

                    var chars = Expression.Parameter(typeof(char[]));

                    var getBytes = typeof(BitConverter)
                        .GetMethod(nameof(BitConverter.GetBytes), new[] { typeof(uint) });

                    var reverse = typeof(Array)
                        .GetMethod(nameof(Array.Reverse), new[] { getBytes.ReturnType });

                    var copyTo = typeof(Array)
                        .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

                    Expression write(Expression value, Expression offset)
                    {
                        Expression component = Expression.Call(null, getBytes, value);

                        if (!BitConverter.IsLittleEndian)
                        {
                            var buffer = Expression.Variable(component.Type);

                            component = Expression.Block(
                                new[] { buffer },
                                Expression.Assign(buffer, component),
                                Expression.Call(null, reverse, buffer),
                                buffer);
                        }

                        return Expression.Call(component, copyTo, chars, offset);
                    }

                    var totalDays = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalDays));
                    var totalMs = typeof(TimeSpan).GetProperty(nameof(TimeSpan.TotalMilliseconds));

                    var encode = typeof(JsonEncodedText)
                        .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                    var writeString = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                    result.Expression = Expression.Block(
                        new[] { chars },
                        Expression.Assign(
                            chars,
                            Expression.NewArrayBounds(typeof(char), Expression.Constant(12))),
                        write(
                            Expression.ConvertChecked(Expression.Property(value, totalDays), typeof(uint)),
                            Expression.Constant(4)),
                        write(
                            Expression.ConvertChecked(
                                Expression.Subtract(
                                    Expression.Convert(Expression.Property(value, totalMs), typeof(ulong)),
                                    Expression.Multiply(
                                        Expression.Convert(Expression.Property(value, totalDays), typeof(ulong)),
                                        Expression.Constant(86400000UL))),
                                typeof(uint)),
                            Expression.Constant(8)),
                        Expression.Call(
                            context.Writer,
                            writeString,
                            Expression.Call(
                                null,
                                encode,
                                Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                                Expression.Constant(ByteEncoder))));
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
    public class EnumSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is EnumSchema enumSchema)
            {
                if (resolution is EnumResolution enumResolution)
                {
                    var writeString = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

                    var symbols = enumSchema.Symbols.ToList();

                    // find a match for each enum in the type:
                    var cases = enumResolution.Symbols.Select(symbol =>
                    {
                        var match = symbols.Find(s => symbol.Name.IsMatch(s));

                        if (match == null)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has a symbol ({symbol.Name}) that cannot be serialized.");
                        }

                        if (symbols.FindLast(s => symbol.Name.IsMatch(s)) != match)
                        {
                            throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.Name} has an ambiguous symbol ({symbol.Name}).");
                        }

                        return Expression.SwitchCase(
                            Expression.Call(context.Writer, writeString, Expression.Constant(match)),
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
    public class FixedSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is FixedSchema fixedSchema)
            {
                var bytes = Expression.Parameter(typeof(byte[]));
                var chars = Expression.Parameter(typeof(char[]));

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                var copyTo = typeof(Array)
                    .GetMethod(nameof(Array.CopyTo), new[] { typeof(Array), typeof(int) });

                var encode = typeof(JsonEncodedText)
                    .GetMethod(nameof(JsonEncodedText.Encode), new[] { typeof(ReadOnlySpan<char>), typeof(JavaScriptEncoder) });

                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(JsonEncodedText) });

                result.Expression = Expression.Block(
                    new[] { bytes, chars },
                    Expression.Assign(bytes, GenerateConversion(value, typeof(byte[]))),
                    Expression.IfThen(
                        Expression.NotEqual(
                            Expression.ArrayLength(bytes),
                            Expression.Constant(fixedSchema.Size)),
                        Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Only arrays of size {fixedSchema.Size} can be serialized to {fixedSchema.Name}.")))),
                    Expression.Assign(chars, Expression.NewArrayBounds(typeof(char), Expression.ArrayLength(bytes))),
                    Expression.Call(bytes, copyTo, chars, Expression.Constant(0)),
                    Expression.Call(
                        context.Writer,
                        writeString,
                        Expression.Call(
                            null,
                            encode,
                            Expression.Convert(chars, typeof(ReadOnlySpan<char>)),
                            Expression.Constant(ByteEncoder))));
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
    public class FloatSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is FloatSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(float) });

                result.Expression = Expression.Call(
                    context.Writer,
                    writeNumber,
                    GenerateConversion(value, typeof(float)));
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="IntSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class IntSerializerBuilderCase : JsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds an int serializer for a type-schema pair.
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
        /// A successful result if the schema is an <see cref="IntSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="long" />.
        /// </exception>
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is IntSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(int) });

                result.Expression = Expression.Call(
                    context.Writer,
                    writeNumber,
                    GenerateConversion(value, typeof(int)));
            }
            else
            {
                result.Exceptions.Add(new UnsupportedSchemaException(schema));
            }

            return result;
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="LongSchema" /> and attempts to map it to
    /// any provided type.
    /// </summary>
    public class LongSerializerBuilderCase : JsonSerializerBuilderCase
    {
        /// <summary>
        /// Builds a long serializer for a type-schema pair.
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
        /// A successful result if the schema is an <see cref="LongSchema" />; an unsuccessful
        /// result otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type cannot be converted to <see cref="long" />.
        /// </exception>
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is LongSchema)
            {
                var writeNumber = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(long) });

                result.Expression = Expression.Call(
                    context.Writer,
                    writeNumber,
                    GenerateConversion(value, typeof(long)));
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
    public class MapSerializerBuilderCase : JsonSerializerBuilderCase
    {
        /// <summary>
        /// The serializer builder to use to build key and value serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new map serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build key and value serializers.
        /// </param>
        public MapSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

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

                    var keySerializer = new KeySerializerVisitor().Visit(SerializerBuilder.BuildExpression(Expression.Property(pair, getKey), new StringSchema(), context));
                    var valueSerializer = SerializerBuilder.BuildExpression(Expression.Property(pair, getValue), mapSchema.Value, context);

                    var writeStartObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartObject), Type.EmptyTypes);

                    var writeEndObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndObject), Type.EmptyTypes);

                    result.Expression = Expression.Block(
                        Expression.Call(context.Writer, writeStartObject),
                        GenerateIteration(value, pair, Expression.Block(keySerializer, valueSerializer)),
                        Expression.Call(context.Writer, writeEndObject));
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
        /// Visits a key serializer to rewrite <see cref="Utf8JsonWriter.WriteStringValue(string)" /> calls.
        /// </summary>
        protected class KeySerializerVisitor : ExpressionVisitor
        {
            private static readonly MethodInfo writePropertyName = typeof(Utf8JsonWriter)
                .GetMethod(nameof(Utf8JsonWriter.WritePropertyName), new[] { typeof(string) });

            private static readonly MethodInfo writeString = typeof(Utf8JsonWriter)
                .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

            /// <summary>
            /// Rewrites a <see cref="Utf8JsonWriter.WriteStringValue(string)" /> call to
            /// <see cref="Utf8JsonWriter.WritePropertyName(string)" />.
            /// </summary>
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method == writeString)
                {
                    return Expression.Call(node.Object, writePropertyName, node.Arguments);
                }

                return node;
            }
        }
    }

    /// <summary>
    /// A serializer builder case that matches <see cref="NullSchema" />.
    /// </summary>
    public class NullSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is NullSchema)
            {
                var writeNull = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNullValue), Type.EmptyTypes);

                result.Expression = Expression.Call(context.Writer, writeNull);
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
    public class RecordSerializerBuilderCase : JsonSerializerBuilderCase
    {
        /// <summary>
        /// The serializer builder to use to build field serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new record serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build field serializers.
        /// </param>
        public RecordSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "JSON serializer builder cannot be null.");
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    // since record serialization is potentially recursive, create a delegate and
                    // return its invocation:
                    var parameter = Expression.Parameter(Expression.GetDelegateType(resolution.Type, context.Writer.Type, typeof(void)));
                    var reference = context.References.GetOrAdd((recordSchema, resolution.Type), parameter);
                    result.Expression = Expression.Invoke(reference, value, context.Writer);

                    var writeStartObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartObject), Type.EmptyTypes);

                    var writePropertyName = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WritePropertyName), new[] { typeof(string) });

                    var writeEndObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndObject), Type.EmptyTypes);

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        var argument = Expression.Variable(resolution.Type);
                        var writes = new List<Expression>();

                        writes.Add(Expression.Call(context.Writer, writeStartObject));

                        foreach (var field in recordSchema.Fields)
                        {
                            var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));

                            if (match == null)
                            {
                                throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type.FullName} does not have a field or property that matches the {field.Name} field on {recordSchema.Name}.");
                            }

                            writes.Add(Expression.Call(context.Writer, writePropertyName, Expression.Constant(field.Name)));
                            writes.Add(SerializerBuilder.BuildExpression(Expression.PropertyOrField(argument, match.Member.Name), field.Type, context));
                        }

                        writes.Add(Expression.Call(context.Writer, writeEndObject));

                        var expression = Expression.Lambda(parameter.Type, Expression.Block(writes), $"{recordSchema.Name} serializer", new[] { argument, context.Writer });

                        if (!context.Assignments.TryAdd(reference, expression))
                        {
                            throw new InvalidOperationException();
                        }
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
    public class StringSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is StringSchema)
            {
                var writeString = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteStringValue), new[] { typeof(string) });

                result.Expression = Expression.Call(
                    context.Writer,
                    writeString,
                    GenerateConversion(value, typeof(string)));
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
    public class TimestampSerializerBuilderCase : JsonSerializerBuilderCase
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

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

                    var writeNumber = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteNumberValue), new[] { typeof(long) });

                    result.Expression = Expression.Call(
                        context.Writer,
                        writeNumber,

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
    public class UnionSerializerBuilderCase : JsonSerializerBuilderCase
    {
        /// <summary>
        /// The serializer builder to use to build child serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Creates a new union serializer builder case.
        /// </summary>
        /// <param name="serializerBuilder">
        /// The serializer builder to use to build child serializers.
        /// </param>
        public UnionSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
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
        public override IJsonSerializerBuildResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, IJsonSerializerBuilderContext context)
        {
            var result = new JsonSerializerBuildResult();

            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema);
                }

                var schemas = unionSchema.Schemas.ToList();
                var candidates = schemas.Where(s => !(s is NullSchema)).ToList();
                var @null = schemas.Find(s => s is NullSchema);

                var writeNull = typeof(Utf8JsonWriter)
                    .GetMethod(nameof(Utf8JsonWriter.WriteNullValue), Type.EmptyTypes);

                Expression expression = null!;

                // if there are non-null schemas, select the first matching one for each possible type:
                if (candidates.Count > 0)
                {
                    var cases = new Dictionary<Type, Expression>();
                    var exceptions = new List<Exception>();

                    var writeStartObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteStartObject), Type.EmptyTypes);

                    var writePropertyName = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WritePropertyName), new[] { typeof(string) });

                    var writeEndObject = typeof(Utf8JsonWriter)
                        .GetMethod(nameof(Utf8JsonWriter.WriteEndObject), Type.EmptyTypes);

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
                                Expression.Call(context.Writer, writeStartObject),
                                Expression.Call(context.Writer, writePropertyName, Expression.Constant(GetSchemaName(candidate))),
                                SerializerBuilder.BuildExpression(Expression.Convert(value, underlying), candidate, context),
                                Expression.Call(context.Writer, writeEndObject));
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
                                Expression.Call(context.Writer, writeNull),
                                body);
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
                    expression = Expression.Call(context.Writer, writeNull);
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
        /// Gets the name of the property used to disambiguate a union.
        /// </summary>
        /// <param name="schema">
        /// A child of the union schema.
        /// </param>
        /// <returns>
        /// If <paramref name="schema" /> is a <see cref="NamedSchema" />, the fully-qualified
        /// name; the type name otherwise.
        /// </returns>
        protected virtual string GetSchemaName(Schema schema)
        {
            return schema switch
            {
                NamedSchema namedSchema => namedSchema.FullName,

                ArraySchema => JsonSchemaToken.Array,
                BooleanSchema => JsonSchemaToken.Boolean,
                BytesSchema => JsonSchemaToken.Bytes,
                DoubleSchema => JsonSchemaToken.Double,
                FloatSchema => JsonSchemaToken.Float,
                IntSchema => JsonSchemaToken.Int,
                LongSchema => JsonSchemaToken.Long,
                MapSchema => JsonSchemaToken.Map,
                StringSchema => JsonSchemaToken.String,

                _ => throw new UnsupportedSchemaException(schema)
            };
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
