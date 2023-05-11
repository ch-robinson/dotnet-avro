namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Diagnostics;
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
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is ArraySchema arraySchema)
            {
                var enumerableType = GetEnumerableType(type);

                if (enumerableType is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    var itemType = enumerableType ?? typeof(object);

                    var collection = Expression.Variable(typeof(ICollection<>).MakeGenericType(itemType), "collection");
                    var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(itemType), "enumerable");
                    var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(itemType), "enumerator");
                    var item = Expression.Parameter(itemType, "item");

                    var loop = Expression.Label();

                    var getCount = collection.Type
                        .GetProperty("Count")
                        .GetGetMethod();

                    var getEnumerator = enumerable.Type
                        .GetMethod("GetEnumerator", Type.EmptyTypes);

                    var getCurrent = enumerator.Type
                        .GetProperty(nameof(IEnumerator.Current))
                        .GetGetMethod();

                    var moveNext = typeof(IEnumerator)
                        .GetMethod(nameof(IEnumerator.MoveNext), Type.EmptyTypes);

                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                    var setItem = Expression.Assign(item, Expression.Property(enumerator, getCurrent));

                    var referenceKey = (arraySchema.Item, itemType);
                    if (!context.References.TryGetValue(referenceKey, out var writeItem))
                    {
                        var argument = Expression.Variable(itemType, "arg");
                        var writeItemExpression = SerializerBuilder.BuildExpression(argument, arraySchema.Item, context, true);

                        if (!context.References.TryGetValue(referenceKey, out writeItem))
                        {
                            writeItem = Expression.Parameter(
                                Expression.GetDelegateType(itemType, context.Writer.Type, typeof(void)));
                            context.References[referenceKey] = writeItem;

                            var itemExpression = Expression.Lambda(
                                writeItem.Type,
                                writeItemExpression,
                                $"{itemType.Name} to {arraySchema.Item} serializer",
                                new[] { argument, context.Writer });

                            context.Assignments.Add(writeItem, itemExpression);
                        }
                    }

                    var dispose = typeof(IDisposable)
                        .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

                    Expression expression;

                    try
                    {
                        expression = BuildConversion(value, collection.Type);
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }

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
                    //                 var item = enumerator.Current
                    //                 <Serialize>(item);
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
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { collection, enumerator, item },
                            Expression.Assign(collection, expression),
                            Expression.IfThen(
                                Expression.GreaterThan(
                                    Expression.Property(collection, getCount),
                                    Expression.Constant(0)),
                                Expression.Block(
                                    Expression.Call(
                                        context.Writer,
                                        writeInteger,
                                        Expression.Convert(
                                            Expression.Property(collection, getCount),
                                            typeof(long))),
                                    Expression.Assign(
                                        enumerator,
                                        Expression.Call(collection, getEnumerator)),
                                    Expression.TryFinally(
                                        Expression.Loop(
                                            Expression.IfThenElse(
                                                Expression.Call(enumerator, moveNext),
                                                Expression.Block(
                                                    setItem,
                                                    Expression.Invoke(writeItem, item, context.Writer)),
                                                Expression.Break(loop)),
                                            loop),
                                        Expression.Call(enumerator, dispose)))),
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
