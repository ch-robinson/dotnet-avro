namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="ArraySchema" />
    /// and attempts to map it to enumerable types.
    /// </summary>
    public class BinaryArraySerializerBuilderCase : ArraySerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryArraySerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build item serializers.
        /// </param>
        public BinaryArraySerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build item serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enumerable type and <paramref name="schema" /> is an <see cref="ArraySchema" />;
        /// an unsuccessful <see cref="BinarySerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not implement <see cref="IEnumerable{T}" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                var itemType = GetEnumerableType(type);

                if (itemType is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    itemType ??= typeof(object);
                    var readOnlyCollectionType = typeof(IReadOnlyCollection<>).MakeGenericType(itemType);
                    var enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);
                    var enumeratorType = typeof(IEnumerator<>).MakeGenericType(itemType);

                    Expression expression;
                    try
                    {
                        if (readOnlyCollectionType.IsAssignableFrom(type))
                        {
                            // NOTE: Not casting the expression to allow us to get the specific enumerator of `type`
                            // This way we can avoid the allocation of an IEnumerator<T> and the overhead of
                            // virtual dispatch calls
                            expression = value;
                        }
                        else
                        {
                            expression = BuildConversion(value, readOnlyCollectionType);
                        }
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }

                    var collection = Expression.Variable(expression.Type);
                    var enumerationReflection = EnumerationReflection.Create(collection, readOnlyCollectionType, enumerableType);

                    var loopLabel = Expression.Label();

                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                    var writeItem = SerializerBuilder
                        .BuildExpression(Expression.Property(enumerationReflection.Enumerator, enumerationReflection.GetCurrent), arraySchema.Item, context);

                    // if (collection.Count > 0)
                    // {
                    //     writer.WriteInteger((long)collection.Count);
                    //
                    //     var enumerator = collection.GetEnumerator();
                    //
                    //     try
                    //     {
                    //         // primitive foreach:
                    //         loop: while (true)
                    //         {
                    //             if (enumerator.MoveNext())
                    //             {
                    //                 ...
                    //             }
                    //             else
                    //             {
                    //                 break loop;
                    //             }
                    //         }
                    //     }
                    //     finally
                    //     {
                    //         enumerator.Dispose();
                    //     }
                    // }
                    //
                    // // write closing block:
                    // writer.WriteInteger(0L);


                    Expression loop = Expression.Loop(
                        Expression.IfThenElse(
                            Expression.Call(enumerationReflection.Enumerator, enumerationReflection.MoveNext),
                            writeItem,
                            Expression.Break(loopLabel)),
                        loopLabel);

                    if (enumerationReflection.DisposeCall is not null)
                    {
                        loop = Expression.TryFinally(loop, enumerationReflection.DisposeCall);
                    }

                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { collection, enumerationReflection.Enumerator },
                            Expression.Assign(collection, expression),
                            Expression.IfThen(
                                Expression.GreaterThan(
                                    Expression.Property(collection, enumerationReflection.GetCount),
                                    Expression.Constant(0)),
                                Expression.Block(
                                    Expression.Call(
                                        context.Writer,
                                        writeInteger,
                                        Expression.Convert(
                                            Expression.Property(collection, enumerationReflection.GetCount),
                                            typeof(long))),
                                    Expression.Assign(
                                        enumerationReflection.Enumerator,
                                        Expression.Call(collection, enumerationReflection.GetEnumerator)),
                                    loop)),
                            Expression.Call(
                                context.Writer,
                                writeInteger,
                                Expression.Constant(0L))));
                }
                else
                {
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryArraySerializerBuilderCase)} can only be applied to enumerable types."));
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryArraySerializerBuilderCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
