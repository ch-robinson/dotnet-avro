namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Builds JSON Avro deserializers for .NET <see cref="Type" />s.
    /// </summary>
    public class JsonDeserializerBuilder : ExpressionBuilder, IJsonDeserializerBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDeserializerBuilder" /> class
        /// configured with the default list of cases.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags the builder should use to select fields and properties.
        /// </param>
        public JsonDeserializerBuilder(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
            : this(CreateDefaultCaseBuilders(memberVisibility))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDeserializerBuilder" /> class
        /// configured with a custom list of cases.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        public JsonDeserializerBuilder(
            IEnumerable<Func<IJsonDeserializerBuilder, IJsonDeserializerBuilderCase>> caseBuilders)
        {
            var cases = new List<IJsonDeserializerBuilderCase>();

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
        public IEnumerable<IJsonDeserializerBuilderCase> Cases { get; }

        /// <summary>
        /// Creates the default list of case builders.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <returns>
        /// A list of case builders that matches most <see cref="Type" />s.
        /// </returns>
        public static IEnumerable<Func<IJsonDeserializerBuilder, IJsonDeserializerBuilderCase>> CreateDefaultCaseBuilders(
            BindingFlags memberVisibility = BindingFlags.Public | BindingFlags.Instance)
        {
            return new Func<IJsonDeserializerBuilder, IJsonDeserializerBuilderCase>[]
            {
                // logical types:
                builder => new JsonDecimalDeserializerBuilderCase(),
                builder => new JsonDurationDeserializerBuilderCase(),
                builder => new JsonTimestampDeserializerBuilderCase(),

                // primitives:
                builder => new JsonBooleanDeserializerBuilderCase(),
                builder => new JsonBytesDeserializerBuilderCase(),
                builder => new JsonDoubleDeserializerBuilderCase(),
                builder => new JsonFixedDeserializerBuilderCase(),
                builder => new JsonFloatDeserializerBuilderCase(),
                builder => new JsonIntDeserializerBuilderCase(),
                builder => new JsonLongDeserializerBuilderCase(),
                builder => new JsonNullDeserializerBuilderCase(),
                builder => new JsonStringDeserializerBuilderCase(),

                // collections:
                builder => new JsonArrayDeserializerBuilderCase(builder),
                builder => new JsonMapDeserializerBuilderCase(builder),

                // enums:
                builder => new JsonEnumDeserializerBuilderCase(),

                // records:
                builder => new JsonRecordDeserializerBuilderCase(builder, memberVisibility),

                // unions:
                builder => new JsonUnionDeserializerBuilderCase(builder),
            };
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonDeserializer<T> BuildDelegate<T>(Schema schema, JsonDeserializerBuilderContext? context = default)
        {
            return BuildDelegateExpression<T>(schema, context).Compile();
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <typeparamref name="T" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression<JsonDeserializer<T>> BuildDelegateExpression<T>(Schema schema, JsonDeserializerBuilderContext? context = default)
        {
            context ??= new JsonDeserializerBuilderContext();

            // ensure that all assignments are present before building the lambda:
            var root = BuildExpression(typeof(T), schema, context);

            var read = typeof(Utf8JsonReader)
                .GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes);

            return Expression.Lambda<JsonDeserializer<T>>(
                Expression.Block(
                    context.Assignments.Keys,
                    context.Assignments
                        .Select(assignment => (Expression)Expression.Assign(assignment.Key, assignment.Value))
                        .Concat(new[] { Expression.Call(context.Reader, read), root })),
                new[] { context.Reader });
        }

        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case can map <paramref name="type" /> to <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual Expression BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
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
