namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Builds binary Avro serializers for .NET <see cref="Type" />s.
    /// </summary>
    public class BinarySerializerBuilder : ExpressionBuilder, IBinarySerializerBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySerializerBuilder" /> class
        /// configured with the default list of cases.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags the builder should use to select fields and properties.
        /// </param>
        public BinarySerializerBuilder(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
            : this(CreateDefaultCaseBuilders(memberVisibility))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySerializerBuilder" /> class
        /// configured with a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        public BinarySerializerBuilder(
            IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> caseBuilders)
        {
            var cases = new List<IBinarySerializerBuilderCase>();

            Cases = cases;

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
        /// Creates the default list of case builders.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <returns>
        /// A list of case builders that matches most <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>> CreateDefaultCaseBuilders(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
        {
            return new Func<IBinarySerializerBuilder, IBinarySerializerBuilderCase>[]
            {
                // logical types:
#if NET6_0_OR_GREATER
                builder => new BinaryDateSerializerBuilderCase(),
#endif
                builder => new BinaryDecimalSerializerBuilderCase(),
                builder => new BinaryDurationSerializerBuilderCase(),
#if NET6_0_OR_GREATER
                builder => new BinaryTimeSerializerBuilderCase(),
#endif
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
                builder => new BinaryRecordSerializerBuilderCase(memberVisibility, builder),

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
            return BuildDelegateExpression<T>(schema, context).Compile();
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression<BinarySerializer<T>> BuildDelegateExpression<T>(Schema schema, BinarySerializerBuilderContext? context = default)
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
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.BuildExpression(value, value.Type, schema, context);

                if (result.Expression != null)
                {
                    return result.Expression;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnsupportedTypeException(value.Type, $"No serializer builder case could be applied to {value.Type}.", new AggregateException(exceptions));
        }
    }
}
