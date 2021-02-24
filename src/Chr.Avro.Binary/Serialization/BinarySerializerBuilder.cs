namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Builds binary Avro serializers for .NET <see cref="Type" />s.
    /// </summary>
    public class BinarySerializerBuilder : IBinarySerializerBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySerializerBuilder" /> class
        /// configured with the default list of cases.
        /// </summary>
        /// <param name="resolver">
        /// The <see cref="ITypeResolver" /> that should be used to retrieve type information. If
        /// no <see cref="ITypeResolver" /> is provided, the <see cref="BinarySerializerBuilder" />
        /// will use a <see cref="TypeResolver" /> with the default set of cases.
        /// </param>
        public BinarySerializerBuilder(ITypeResolver? resolver = default)
            : this(CreateDefaultCaseBuilders(), resolver)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySerializerBuilder" /> class
        /// configured with a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="resolver">
        /// The <see cref="ITypeResolver" /> that should be used to retrieve type information. If
        /// no <see cref="ITypeResolver" /> is provided, the <see cref="BinarySerializerBuilder" />
        /// will use a <see cref="TypeResolver" /> with the default set of cases.
        /// </param>
        public BinarySerializerBuilder(
            IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> caseBuilders,
            ITypeResolver? resolver = default)
        {
            var cases = new List<IBinarySerializerBuilderCase>();

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
        public IEnumerable<IBinarySerializerBuilderCase> Cases { get; }

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
        public static IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> CreateDefaultCaseBuilders()
        {
            return new Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>[]
            {
                // logical types:
                builder => new BinaryDecimalSerializerBuilderCase(),
                builder => new BinaryDurationSerializerBuilderCase(),
                builder => new BinaryTimestampSerializerBuilderCase(),

                // primitives:
                builder => new BinaryBooleanSerializerBuilderCase(),
                builder => new BinaryBytesSerializerBuilderCase(),
                builder => new BinaryDoubleSerializerBuilderCase(),
                builder => new BinaryFixedSerializerBuilderCase(),
                builder => new BinaryFloatSerializerBuilderCase(),
                builder => new BinaryIntSerializerBuilderCase(),
                builder => new BinaryLongSerializerBuilderCase(),
                builder => new BinaryNullSerializerBuilderCase(),
                builder => new BinaryStringSerializerBuilderCase(),

                // collections:
                builder => new BinaryArraySerializerBuilderCase(builder),
                builder => new BinaryMapSerializerBuilderCase(builder),

                // enums:
                builder => new BinaryEnumSerializerBuilderCase(),

                // records:
                builder => new BinaryRecordSerializerBuilderCase(builder),

                // unions:
                builder => new BinaryUnionSerializerBuilderCase(builder),
            };
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializer<T> BuildDelegate<T>(Schema schema, BinarySerializerBuilderContext? context = default)
        {
            return BuildExpression<T>(schema, context).Compile();
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression<BinarySerializer<T>> BuildExpression<T>(Schema schema, BinarySerializerBuilderContext? context = default)
        {
            context ??= new BinarySerializerBuilderContext();
            var value = Expression.Parameter(typeof(T));

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(value, schema, context);

            return Expression.Lambda<BinarySerializer<T>>(
                Expression.Block(
                    context.Assignments.Keys,
                    context.Assignments
                        .Select(assignment => (Expression)Expression.Assign(assignment.Key, assignment.Value))
                        .Concat(new[] { root })),
                new[] { value, context.Writer });
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map the <see cref="Expression.Type" /> of <paramref name="value" />
        /// to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression BuildExpression(Expression value, Schema schema, BinarySerializerBuilderContext context)
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
