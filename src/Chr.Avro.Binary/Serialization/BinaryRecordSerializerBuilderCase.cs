namespace Chr.Avro.Serialization
{
    using System;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class BinaryRecordSerializerBuilderCase : RecordSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="memberVisibility">
        /// The binding flags to use to select fields and properties.
        /// </param>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build field serializers.
        /// </param>
        public BinaryRecordSerializerBuilderCase(
            BindingFlags memberVisibility,
            IBinarySerializerBuilder serializerBuilder)
        {
            MemberVisibility = memberVisibility;
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the binding flags used to select fields and properties.
        /// </summary>
        public BindingFlags MemberVisibility { get; }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build field serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is not an array or primitive type and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="BinarySerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not have a matching member for each
        /// <see cref="RecordField" /> on <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (!type.IsArray && !type.IsPrimitive)
                {
                    // since record serialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(type, context.Writer.Type, typeof(void)));

                    ParameterExpression reference;
                    if (registerExpression)
                    {
                        if (!context.References.TryGetValue((recordSchema, type), out reference))
                        {
                            context.References.Add((recordSchema, type), reference = parameter);
                        }
                    }
                    else
                    {
                        reference = parameter;
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        var members = type.GetMembers(MemberVisibility);

                        var argument = Expression.Variable(type);
                        var writes = recordSchema.Fields
                            .Select(field =>
                            {
                                var match = members.SingleOrDefault(member => IsMatch(field, member));

                                Expression inner;

                                if (match == null)
                                {
                                    // if the type could be dynamic, attempt to use a dynamic getter:

                                    // Temporary hack: if this is not the typical case where value is a simple lambda parameter,
                                    // do not attempt to do any dynamic getting from it, as it might be a expression referencing
                                    // parameters, which would result in an incorrect lambda expression store in context.Assignments for that type
                                    // That caters for the case where we end up here while iterating through an empty array of type[],
                                    // I imagine there might be situations where this is not what we want to do
                                    if ((typeof(IDynamicMetaObjectProvider).IsAssignableFrom(type) || type == typeof(object)))
                                    {
                                        if (value.NodeType == ExpressionType.Parameter)
                                        {
                                            inner = this.BuildDynamicGet(argument, field.Name);
                                        }
                                        else
                                        {
                                            inner = Expression.Constant(null);
                                        }
                                    }
                                    else
                                    {
                                        if (field.Default is not null)
                                        {
                                            inner = Expression.Constant(field.Default.ToObject<dynamic>());
                                        }
                                        else
                                        {
                                            throw new UnsupportedTypeException(type, $"{type} does not have a field or property that matches the {field.Name} field on {recordSchema.FullName}.");
                                        }
                                    }
                                }
                                else
                                {
                                    inner = Expression.PropertyOrField(argument, match.Name);
                                }

                                try
                                {
                                    return SerializerBuilder.BuildExpression(inner, field.Type, context, registerExpression);
                                }
                                catch (Exception exception)
                                {
                                    throw new UnsupportedTypeException(type, $"{(match is null ? "A" : $"The {match.Name}")} member on {type} could not be mapped to the {field.Name} field on {recordSchema.FullName}.", exception);
                                }
                            })
                            .ToList();

                        // .NET Framework doesn’t permit empty block expressions:
                        var expression = writes.Count > 0
                            ? Expression.Block(writes)
                            : Expression.Empty() as Expression;

                        expression = Expression.Lambda(
                            parameter.Type,
                            expression,
                            $"{type.Name} to {recordSchema.Name} serializer",
                            new[] { argument, context.Writer });

                        if (registerExpression)
                        {
                            context.Assignments.Add(reference, expression);
                        }
                    }

                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Invoke(reference, value, context.Writer));
                }
                else
                {
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryRecordSerializerBuilderCase)} cannot be applied to array or primitive types."));
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryRecordSerializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }
    }
}
