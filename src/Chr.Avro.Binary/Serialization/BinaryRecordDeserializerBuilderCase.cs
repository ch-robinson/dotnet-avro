namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

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
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        public BinaryRecordDeserializerBuilderCase(
            IBinaryDeserializerBuilder deserializerBuilder,
            BindingFlags memberVisibility)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
            MemberVisibility = memberVisibility;
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build field deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is not an array or primitive type and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                var underlying = Nullable.GetUnderlyingType(type) ?? type;

                if (!underlying.IsArray && !underlying.IsPrimitive)
                {
                    // since record deserialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(context.Reader.Type.MakeByRefType(), underlying));

                    if (!context.References.TryGetValue((recordSchema, type), out var reference))
                    {
                        context.References.Add((recordSchema, type), reference = parameter);
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        Expression expression;

                        if (GetRecordConstructor(underlying, recordSchema) is ConstructorInfo constructor)
                        {
                            var parameters = constructor.GetParameters();

                            // map constructor parameters to fields:
                            var mapping = recordSchema.Fields
                                .Select(field =>
                                {
                                    // there will be a match or we wouldn’t have made it this far:
                                    var match = parameters.Single(parameter => IsMatch(field, parameter.Name));
                                    var parameter = Expression.Parameter(match.ParameterType);

                                    return (
                                        Match: match,
                                        Parameter: parameter,
                                        Assignment: (Expression)Expression.Assign(
                                            parameter,
                                            DeserializerBuilder.BuildExpression(match.ParameterType, field.Type, context)));
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
                                            constructor,
                                            parameters
                                                .Select(parameter => mapping.ContainsKey(parameter)
                                                    ? (Expression)mapping[parameter].Parameter
                                                    : Expression.Constant(parameter.DefaultValue))),
                                    }));
                        }
                        else
                        {
                            var members = underlying.GetMembers(MemberVisibility);
                            var value = Expression.Parameter(underlying);

                            expression = Expression.Block(
                                new[] { value },
                                new[] { (Expression)Expression.Assign(value, Expression.New(value.Type)) }
                                    .Concat(recordSchema.Fields.Select(field =>
                                    {
                                        var match = members.SingleOrDefault(member => IsMatch(field, member.Name));
                                        var schema = match == null ? CreateSurrogateSchema(field.Type) : field.Type;
                                        var type = match == null ? GetSurrogateType(schema) : match switch
                                        {
                                            FieldInfo fieldMatch => fieldMatch.FieldType,
                                            PropertyInfo propertyMatch => propertyMatch.PropertyType,
                                            MemberInfo unknown => throw new InvalidOperationException($"Record fields can only be mapped to fields and properties.")
                                        };

                                        // always read to advance the stream:
                                        var expression = DeserializerBuilder.BuildExpression(type, schema, context);

                                        if (match != null)
                                        {
                                            // and assign if a field matches:
                                            expression = Expression.Assign(Expression.PropertyOrField(value, match.Name), expression);
                                        }

                                        return expression;
                                    }))
                                    .Concat(new[] { Expression.ConvertChecked(value, type) }));
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
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryRecordDeserializerBuilderCase)} cannot be applied to array or primitive types."));
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
