namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="RecordSchema" />
    /// and attempts to map it to classes or structs.
    /// </summary>
    public class BinaryRecordSerializerBuilderCase : RecordSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryRecordSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build field serializers.
        /// </param>
        public BinaryRecordSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build field serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="RecordSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is a <see cref="RecordResolution" /> and <paramref name="schema" /> is a <see cref="RecordSchema" />;
        /// an unsuccessful <see cref="BinarySerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> does not have a matching member for each
        /// <see cref="RecordField" /> on <paramref name="schema" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, TypeResolution resolution, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is RecordSchema recordSchema)
            {
                if (resolution is RecordResolution recordResolution)
                {
                    // since record serialization is potentially recursive, create a top-level
                    // reference:
                    var parameter = Expression.Parameter(
                        Expression.GetDelegateType(resolution.Type, context.Writer.Type, typeof(void)));

                    if (!context.References.TryGetValue((recordSchema, resolution.Type), out var reference))
                    {
                        context.References.Add((recordSchema, resolution.Type), reference = parameter);
                    }

                    // then build/set the delegate if it hasn’t been built yet:
                    if (parameter == reference)
                    {
                        var argument = Expression.Variable(resolution.Type);
                        var writes = recordSchema.Fields
                            .Select(field =>
                            {
                                var match = recordResolution.Fields.SingleOrDefault(f => f.Name.IsMatch(field.Name));

                                if (match == null)
                                {
                                    throw new UnsupportedTypeException(resolution.Type, $"{resolution.Type} does not have a field or property that matches the {field.Name} field on {recordSchema.Name}.");
                                }

                                return SerializerBuilder.BuildExpression(Expression.PropertyOrField(argument, match.Member.Name), field.Type, context);
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
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(BinaryRecordSerializerBuilderCase)} can only be applied to {nameof(RecordResolution)}s."));
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryRecordSerializerBuilderCase)} can only be applied to {nameof(RecordSchema)}s."));
            }
        }
    }
}
