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
    /// Implements a <see cref="JsonSerializerBuilder" /> case that matches <see cref="UnionSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class JsonUnionSerializerBuilderCase : UnionSerializerBuilderCase, IJsonSerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonUnionSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build child serializers.
        /// </param>
        public JsonUnionSerializerBuilderCase(IJsonSerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder;
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build child serializers.
        /// </summary>
        public IJsonSerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonSerializer{T}" /> for a <see cref="UnionSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonSerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="UnionSchema" />; an unsuccessful <see cref="JsonSerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> has no <see cref="UnionSchema.Schemas" />.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="resolution" /> cannot be mapped to at least one
        /// <see cref="Schema" /> in <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual JsonSerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, JsonSerializerBuilderContext context)
        {
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
                            $"{resolution.Type.Name} does not match any non-null members of {unionSchema}.",
                            new AggregateException(exceptions));
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
                            Expression.Constant($"Unexpected type encountered serializing to {resolution.Type}.")));

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

                return JsonSerializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return JsonSerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonUnionSerializerBuilderCase)} can only be applied to {nameof(UnionSchema)}s."));
            }
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
    }
}
