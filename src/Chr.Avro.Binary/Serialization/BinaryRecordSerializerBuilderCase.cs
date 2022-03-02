namespace Chr.Avro.Serialization
{
    using System;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;
    using Microsoft.CSharp.RuntimeBinder;

    using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

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
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (!type.IsArray && !type.IsPrimitive)
                {
                    // since record serialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(type, context.Writer.Type, typeof(void)));

                    if (!context.References.TryGetValue((recordSchema, type), out var reference))
                    {
                        context.References.Add((recordSchema, type), reference = parameter);
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
                                    if (typeof(IDynamicMetaObjectProvider).IsAssignableFrom(type) || type == typeof(object))
                                    {
                                        var flags = CSharpBinderFlags.None;
                                        var infos = new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) };
                                        var binder = Binder.GetMember(flags, field.Name, type, infos);
                                        inner = Expression.Dynamic(binder, typeof(object), value);
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
                                    return SerializerBuilder.BuildExpression(inner, field.Type, context);
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
                            $"{recordSchema.Name} serializer",
                            new[] { argument, context.Writer });

                        context.Assignments.Add(reference, expression);
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
