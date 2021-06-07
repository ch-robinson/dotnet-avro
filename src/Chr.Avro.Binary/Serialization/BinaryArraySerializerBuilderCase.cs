namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
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
                if (GetEnumerableType(type) is Type itemType)
                {
                    var collection = Expression.Variable(typeof(ICollection<>).MakeGenericType(itemType));
                    var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(itemType));
                    var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(itemType));
                    var loop = Expression.Label();

                    var toList = typeof(Enumerable)
                        .GetMethod(nameof(Enumerable.ToList))
                        .MakeGenericMethod(itemType);

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

                    var writeItem = SerializerBuilder
                        .BuildExpression(Expression.Property(enumerator, getCurrent), arraySchema.Item, context);

                    var dispose = typeof(IDisposable)
                        .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

                    Expression expression;

                    try
                    {
                        expression = Expression.Assign(
                            collection,
                            Expression.Condition(
                                Expression.TypeIs(value, collection.Type),
                                Expression.Convert(value, collection.Type),
                                Expression.Convert(
                                    Expression.Call(
                                        null,
                                        toList,
                                        Expression.Convert(value, enumerable.Type)),
                                    collection.Type)));
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }

                    // var collection = value is ICollection<T>
                    //     ? (ICollection<T>)value
                    //     : (ICollection<T>)value.ToList();
                    //
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
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Block(
                            new[] { collection, enumerator },
                            expression,
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
                                                writeItem,
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
