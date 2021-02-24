namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Builds JSON Avro serializers for .NET <see cref="Type" />s.
    /// </summary>
    public class JsonSerializerBuilder : IJsonSerializerBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSerializerBuilder" /> class
        /// configured with the default list of cases.
        /// </summary>
        /// <param name="resolver">
        /// The <see cref="ITypeResolver" /> that should be used to retrieve type information. If
        /// no <see cref="ITypeResolver" /> is provided, the <see cref="JsonSerializerBuilder" />
        /// will use a <see cref="TypeResolver" /> with the default set of cases.
        /// </param>
        public JsonSerializerBuilder(ITypeResolver? resolver = null)
            : this(CreateDefaultCaseBuilders(), resolver)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSerializerBuilder" /> class
        /// configured with a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolver">
        /// The <see cref="ITypeResolver" /> that should be used to retrieve type information. If
        /// no <see cref="ITypeResolver" /> is provided, the <see cref="JsonSerializerBuilder" />
        /// will use a <see cref="TypeResolver" /> with the default set of cases.
        /// </param>
        public JsonSerializerBuilder(
            IEnumerable<Func<IJsonSerializerBuilder, IJsonSerializerBuilderCase>> caseBuilders,
            ITypeResolver? resolver = null)
        {
            var cases = new List<IJsonSerializerBuilderCase>();

            Cases = cases;
            Resolver = resolver ?? new TypeResolver();

            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Gets the list of cases that the serializer builder will attempt to apply. If the first
        /// case does not match, the serializer builder will try the next case, and so on until all
        /// cases have been tried.
        /// </summary>
        public IEnumerable<IJsonSerializerBuilderCase> Cases { get; }

        /// <summary>
        /// Gets the resolver that will be used to retrieve type information.
        /// </summary>
        public ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates the default list of case builders.
        /// </summary>
        /// <returns>
        /// A list of case builders that matches most <see cref="TypeResolution" />s.
        /// </returns>
        public static IEnumerable<Func<IJsonSerializerBuilder, IJsonSerializerBuilderCase>> CreateDefaultCaseBuilders()
        {
            return new Func<IJsonSerializerBuilder, IJsonSerializerBuilderCase>[]
            {
                // logical types:
                builder => new JsonDecimalSerializerBuilderCase(),
                builder => new JsonDurationSerializerBuilderCase(),
                builder => new JsonTimestampSerializerBuilderCase(),

                // primitives:
                builder => new JsonBooleanSerializerBuilderCase(),
                builder => new JsonBytesSerializerBuilderCase(),
                builder => new JsonDoubleSerializerBuilderCase(),
                builder => new JsonFixedSerializerBuilderCase(),
                builder => new JsonFloatSerializerBuilderCase(),
                builder => new JsonIntSerializerBuilderCase(),
                builder => new JsonLongSerializerBuilderCase(),
                builder => new JsonNullSerializerBuilderCase(),
                builder => new JsonStringSerializerBuilderCase(),

                // collections:
                builder => new JsonArraySerializerBuilderCase(builder),
                builder => new JsonMapSerializerBuilderCase(builder),

                // enums:
                builder => new JsonEnumSerializerBuilderCase(),

                // records:
                builder => new JsonRecordSerializerBuilderCase(builder),

                // unions:
                builder => new JsonUnionSerializerBuilderCase(builder),
            };
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializer<T> BuildDelegate<T>(Schema schema, JsonSerializerBuilderContext? context = default)
        {
            return BuildExpression<T>(schema).Compile();
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression<JsonSerializer<T>> BuildExpression<T>(Schema schema, JsonSerializerBuilderContext? context = default)
        {
            context ??= new JsonSerializerBuilderContext();
            var value = Expression.Parameter(typeof(T));

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(value, schema, context);

            var flush = typeof(Utf8JsonWriter)
                .GetMethod(nameof(Utf8JsonWriter.Flush), Type.EmptyTypes);

            return Expression.Lambda<JsonSerializer<T>>(
                Expression.Block(
                    context.Assignments.Keys,
                    context.Assignments
                        .Select(a => (Expression)Expression.Assign(a.Key, a.Value))
                        .Concat(new[] { root, Expression.Call(context.Writer, flush) })),
                new[] { value, context.Writer });
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the <see cref="Expression.Type" /> of <paramref name="value" />
        /// to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression BuildExpression(Expression value, Schema schema, JsonSerializerBuilderContext context)
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

            throw new UnsupportedTypeException(resolution.Type, $"No serializer builder case matched {resolution.Type} (as {resolution.GetType().Name}).", new AggregateException(exceptions));
        }
    }
}
