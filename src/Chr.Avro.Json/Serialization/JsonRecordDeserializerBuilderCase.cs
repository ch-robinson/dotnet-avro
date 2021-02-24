namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class JsonRecordDeserializerBuilderCase : RecordDeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonRecordDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build field deserializers.
        /// </param>
        public JsonRecordDeserializerBuilderCase(IJsonDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "JSON deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build field deserializers.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

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
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    // since record deserialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(context.Reader.Type.MakeByRefType(), resolution.Type));

                    if (!context.References.TryGetValue((recordSchema, resolution.Type), out var reference))
                    {
                        context.References.Add((recordSchema, resolution.Type), reference = parameter);
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        Expression expression;

                        var loop = Expression.Label();

                        var tokenType = typeof(Utf8JsonReader)
                            .GetProperty(nameof(Utf8JsonReader.TokenType));

                        var getUnexpectedTokenException = typeof(JsonExceptionHelper)
                            .GetMethod(nameof(JsonExceptionHelper.GetUnexpectedTokenException));

                        var read = typeof(Utf8JsonReader)
                            .GetMethod(nameof(Utf8JsonReader.Read), Type.EmptyTypes);

                        var getString = typeof(Utf8JsonReader)
                            .GetMethod(nameof(Utf8JsonReader.GetString), Type.EmptyTypes);

                        var getUnknownRecordFieldException = typeof(JsonExceptionHelper)
                            .GetMethod(nameof(JsonExceptionHelper.GetUnknownRecordFieldException));

                        if (FindRecordConstructor(recordResolution, recordSchema) is ConstructorResolution constructorResolution)
                        {
                            // map constructor parameters to fields:
                            var mapping = recordSchema.Fields
                                .Select(field =>
                                {
                                    // there will be a match or we wouldn’t have made it this far:
                                    var match = constructorResolution.Parameters.Single(f => f.Name.IsMatch(field.Name));
                                    var parameter = Expression.Parameter(match.Parameter.ParameterType);

                                    return (
                                        Field: field,
                                        Match: match,
                                        Parameter: parameter,
                                        Assignment: (Expression)Expression.Block(
                                            Expression.Call(context.Reader, read),
                                            Expression.Assign(
                                                parameter,
                                                DeserializerBuilder.BuildExpression(match.Parameter.ParameterType, field.Type, context))));
                                })
                                .ToDictionary(r => r.Match);

                            expression = Expression.Block(
                                mapping
                                    .Select(d => d.Value.Parameter),
                                Expression.IfThen(
                                    Expression.NotEqual(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.StartObject)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.StartObject })))),
                                Expression.Loop(
                                    Expression.Block(
                                        Expression.Call(context.Reader, read),
                                        Expression.IfThen(
                                            Expression.Equal(
                                                Expression.Property(context.Reader, tokenType),
                                                Expression.Constant(JsonTokenType.EndObject)),
                                            Expression.Break(loop)),
                                        Expression.Switch(
                                            Expression.Call(context.Reader, getString),
                                            Expression.Throw(
                                                Expression.Call(
                                                    null,
                                                    getUnknownRecordFieldException,
                                                    context.Reader)),
                                            mapping
                                                .Select(pair =>
                                                    Expression.SwitchCase(
                                                        Expression.Block(pair.Value.Assignment, Expression.Empty()),
                                                        Expression.Constant(pair.Value.Field.Name)))
                                                .ToArray())),
                                    loop),
                                Expression.New(
                                    constructorResolution.Constructor,
                                    constructorResolution.Parameters
                                        .Select(parameter => mapping.ContainsKey(parameter)
                                            ? (Expression)mapping[parameter].Parameter
                                            : Expression.Constant(parameter.Parameter.DefaultValue))));
                        }
                        else
                        {
                            var value = Expression.Parameter(resolution.Type);

                            expression = Expression.Block(
                                new[] { value },
                                Expression.Assign(value, Expression.New(value.Type)),
                                Expression.IfThen(
                                    Expression.NotEqual(
                                        Expression.Property(context.Reader, tokenType),
                                        Expression.Constant(JsonTokenType.StartObject)),
                                    Expression.Throw(
                                        Expression.Call(
                                            null,
                                            getUnexpectedTokenException,
                                            context.Reader,
                                            Expression.Constant(new[] { JsonTokenType.StartObject })))),
                                Expression.Loop(
                                    Expression.Block(
                                        Expression.Call(context.Reader, read),
                                        Expression.IfThen(
                                            Expression.Equal(
                                                Expression.Property(context.Reader, tokenType),
                                                Expression.Constant(JsonTokenType.EndObject)),
                                            Expression.Break(loop)),
                                        Expression.Switch(
                                            Expression.Call(context.Reader, getString),
                                            Expression.Throw(
                                                Expression.Call(
                                                    null,
                                                    getUnknownRecordFieldException,
                                                    context.Reader)),
                                            recordSchema.Fields
                                                .Select(field =>
                                                {
                                                    var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));
                                                    var schema = match == null ? CreateSurrogateSchema(field.Type) : field.Type;
                                                    var type = match == null ? GetSurrogateType(schema) : match.Type;

                                                    // always read to advance the stream:
                                                    Expression expression = Expression.Block(
                                                        Expression.Call(context.Reader, read),
                                                        DeserializerBuilder.BuildExpression(type, schema, context));

                                                    if (match != null)
                                                    {
                                                        // and assign if a field matches:
                                                        expression = Expression.Assign(Expression.PropertyOrField(value, match.Member.Name), expression);
                                                    }

                                                    return Expression.SwitchCase(
                                                        Expression.Block(expression, Expression.Empty()),
                                                        Expression.Constant(field.Name));
                                                })
                                                .ToArray())),
                                    loop),
                                value);
                        }

                        expression = Expression.Lambda(
                            parameter.Type,
                            expression,
                            $"{recordSchema.Name} deserializer",
                            new[] { context.Reader });

                        context.Assignments.Add(reference, expression);
                    }

                    return JsonDeserializerBuilderCaseResult.FromExpression(
                        Expression.Invoke(reference, context.Reader));
                }
                else
                {
                    return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(JsonRecordDeserializerBuilderCase)} can only be applied to {nameof(RecordResolution)}s."));
                }
            }
            else
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(JsonRecordDeserializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
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
                EnumSchema _ => new StringSchema(),
                MapSchema map => new MapSchema(CreateSurrogateSchema(map.Value)),
                UnionSchema union => new UnionSchema(union.Schemas.Select(CreateSurrogateSchema).ToList()),
                _ => schema
            };
        }
    }
}
