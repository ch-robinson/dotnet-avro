namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class BinaryRecordDeserializerBuilderCase : RecordDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build field deserializers.
        /// </param>
        public BinaryRecordDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build field deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is a <see ref="RecordResolution" /> and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved type is not assignable from any supported array or collection
        /// type and does not have a constructor that can be used to instantiate it.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, BinaryDeserializerBuilderContext context)
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
                                        Match: match,
                                        Parameter: parameter,
                                        Assignment: (Expression)Expression.Assign(
                                            parameter,
                                            DeserializerBuilder.BuildExpression(match.Parameter.ParameterType, field.Type, context)));
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
                                                    : Expression.Constant(parameter.Parameter.DefaultValue))),
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
                                        var type = match == null ? GetSurrogateType(schema) : match.Type;

                                        // always read to advance the stream:
                                        var expression = DeserializerBuilder.BuildExpression(type, schema, context);

                                        if (match != null)
                                        {
                                            // and assign if a field matches:
                                            expression = Expression.Assign(Expression.PropertyOrField(value, match.Member.Name), expression);
                                        }

                                        return expression;
                                    }))
                                    .Concat(new[] { value }));
                        }

                        expression = Expression.Lambda(
                            parameter.Type,
                            expression,
                            $"{recordSchema.Name} deserializer",
                            new[] { context.Reader });

                        context.Assignments.Add(reference, expression);
                    }

                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        Expression.Invoke(reference, context.Reader));
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(BinaryRecordDeserializerBuilderCase)} can only be applied to {nameof(RecordResolution)}s."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryRecordDeserializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
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
                EnumSchema _ => new LongSchema(),
                MapSchema map => new MapSchema(CreateSurrogateSchema(map.Value)),
                UnionSchema union => new UnionSchema(union.Schemas.Select(CreateSurrogateSchema).ToList()),
                _ => schema
            };
        }
    }
}
