namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Builds binary Avro deserializers for .NET <see cref="Type" />s.
    /// </summary>
    public class BinaryDeserializerBuilder : ExpressionBuilder, IBinaryDeserializerBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryDeserializerBuilder" /> class
        /// configured with the default list of cases.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags the builder should use to select fields and properties.
        /// </param>
        public BinaryDeserializerBuilder(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
            : this(CreateDefaultCaseBuilders(memberVisibility))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryDeserializerBuilder" /> class
        /// configured with a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        public BinaryDeserializerBuilder(
            IEnumerable<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> caseBuilders)
        {
            var cases = new List<IBinaryDeserializerBuilderCase>();

            Cases = cases;

            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
        }

        /// <summary>
        /// Gets the list of cases that the deserializer builder will attempt to apply. If the
        /// first case does not match, the deserializer builder will try the next case, and so on
        /// until all cases have been tried.
        /// </summary>
        public IEnumerable<IBinaryDeserializerBuilderCase> Cases { get; }

        /// <summary>
        /// Creates the default list of case builders.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <returns>
        /// A list of case builders that matches most <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>> CreateDefaultCaseBuilders(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
        {
            return new Func<IBinaryDeserializerBuilder, IBinaryDeserializerBuilderCase>[]
            {
                // logical types:
#if NET6_0_OR_GREATER
                builder => new BinaryDateDeserializerBuilderCase(),
#endif
                builder => new BinaryDecimalDeserializerBuilderCase(),
                builder => new BinaryDurationDeserializerBuilderCase(),
#if NET6_0_OR_GREATER
                builder => new BinaryTimeDeserializerBuilderCase(),
#endif
                builder => new BinaryTimestampDeserializerBuilderCase(),

                // primitives:
                builder => new BinaryBooleanDeserializerBuilderCase(),
                builder => new BinaryBytesDeserializerBuilderCase(),
                builder => new BinaryDoubleDeserializerBuilderCase(),
                builder => new BinaryFixedDeserializerBuilderCase(),
                builder => new BinaryFloatDeserializerBuilderCase(),
                builder => new BinaryIntDeserializerBuilderCase(),
                builder => new BinaryLongDeserializerBuilderCase(),
                builder => new BinaryNullDeserializerBuilderCase(),
                builder => new BinaryStringDeserializerBuilderCase(),

                // collections:
                builder => new BinaryArrayDeserializerBuilderCase(builder),
                builder => new BinaryMapDeserializerBuilderCase(builder),

                // enums:
                builder => new BinaryEnumDeserializerBuilderCase(),

                // records:
                builder => new BinaryRecordDeserializerBuilderCase(builder, memberVisibility),

                // unions:
                builder => new BinaryUnionDeserializerBuilderCase(builder),
            };
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializer<T> BuildDelegate<T>(Schema schema, BinaryDeserializerBuilderContext? context = default)
        {
            return BuildDelegateExpression<T>(schema, context).Compile();
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression<BinaryDeserializer<T>> BuildDelegateExpression<T>(Schema schema, BinaryDeserializerBuilderContext? context = default)
        {
            context ??= new BinaryDeserializerBuilderContext();

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(typeof(T), schema, context);

            return Expression.Lambda<BinaryDeserializer<T>>(
                Expression.Block(
                    context.Assignments.Keys,
                    context.Assignments
                        .Select(assignment => (Expression)Expression.Assign(assignment.Key, assignment.Value))
                        .Concat(new[] { root })),
                new[] { context.Reader });
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <paramref name="type" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            var exceptions = new List<Exception>();

            foreach (var @case in Cases)
            {
                var result = @case.BuildExpression(type, schema, context);

                if (result.Expression != null)
                {
                    return result.Expression;
                }

                exceptions.AddRange(result.Exceptions);
            }

            throw new UnsupportedTypeException(type, $"No deserializer builder case could be applied to {type}.", new AggregateException(exceptions));
        }
    }
}
