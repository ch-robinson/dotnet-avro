namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="UnionSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryUnionSerializerBuilderCase : UnionSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryUnionSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build child serializers.
        /// </param>
        public BinaryUnionSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder;
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build child serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="UnionSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="UnionSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> has no <see cref="UnionSchema.Schemas" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be mapped to at least one <see cref="Schema" />
        /// in <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is UnionSchema unionSchema)
            {
                if (unionSchema.Schemas.Count < 1)
                {
                    throw new UnsupportedSchemaException(schema, "A serializer cannot be built for an empty union.");
                }

                var schemas = unionSchema.Schemas.ToList();
                var candidates = schemas.Where(s => s is not NullSchema).ToList();
                var @null = schemas.Find(s => s is NullSchema);

                var writeInteger = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                Expression expression;

                // if there are non-null schemas, select the first matching one for each possible type:
                if (candidates.Count > 0)
                {
                    var cases = new Dictionary<Type, Expression>();
                    var exceptions = new List<Exception>();

                    foreach (var candidate in candidates)
                    {
                        var selected = SelectType(type, candidate);

                        if (cases.ContainsKey(selected))
                        {
                            continue;
                        }

                        var underlying = Nullable.GetUnderlyingType(selected) ?? selected;

                        Expression body;

                        try
                        {
                            body = Expression.Block(
                                Expression.Call(
                                    context.Writer,
                                    writeInteger,
                                    Expression.Constant((long)schemas.IndexOf(candidate))),
                                SerializerBuilder.BuildExpression(Expression.Convert(value, underlying), candidate, context, registerExpression));
                        }
                        catch (Exception exception)
                        {
                            exceptions.Add(exception);
                            continue;
                        }

                        cases.Add(selected, body);
                    }

                    if (cases.Count == 0)
                    {
                        throw new UnsupportedTypeException(
                            type,
                            $"{type.Name} does not match any non-null members of {unionSchema}.",
                            new AggregateException(exceptions));
                    }

                    if (cases.Count == 1 && cases.First() is var first && first.Key == type)
                    {
                        expression = first.Value;
                    }
                    else
                    {
                        var exceptionConstructor = typeof(InvalidOperationException)
                            .GetConstructor(new[] { typeof(string) });

                        expression = Expression.Throw(Expression.New(
                            exceptionConstructor,
                            Expression.Constant($"Unexpected type encountered serializing to {type}.")));

                        foreach (var @case in cases)
                        {
                            expression = Expression.IfThenElse(
                                Expression.TypeIs(value, @case.Key),
                                @case.Value,
                                expression);
                        }
                    }

                    if (@null != null && !(type.IsValueType && Nullable.GetUnderlyingType(type) == null))
                    {
                        expression = Expression.IfThenElse(
                            Expression.Equal(value, Expression.Constant(null, type)),
                            Expression.Call(
                                context.Writer,
                                writeInteger,
                                Expression.Constant((long)schemas.IndexOf(@null))),
                            expression);
                    }
                }

                // otherwise, we know that the schema is just ["null"]:
                else
                {
                    expression = Expression.Call(
                        context.Writer,
                        writeInteger,
                        Expression.Constant((long)schemas.IndexOf(@null)));
                }

                return BinarySerializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryUnionSerializerBuilderCase)} can only be applied to {nameof(UnionSchema)}s."));
            }
        }
    }
}
