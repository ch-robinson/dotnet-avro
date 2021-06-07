namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="MapSchema" />
    /// and attempts to map it to dictionary types.
    /// </summary>
    public class BinaryMapSerializerBuilderCase : MapSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryMapSerializerBuilderCase" /> class.
        /// </summary>
        /// <param name="serializerBuilder">
        /// A serializer builder instance that will be used to build key and value serializers.
        /// </param>
        public BinaryMapSerializerBuilderCase(IBinarySerializerBuilder serializerBuilder)
        {
            SerializerBuilder = serializerBuilder ?? throw new ArgumentNullException(nameof(serializerBuilder), "Binary serializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the serializer builder instance that will be used to build key and value serializers.
        /// </summary>
        public IBinarySerializerBuilder SerializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for an <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="type" />
        /// is a dictionary type and <paramref name="schema" /> is a <see cref="MapSchema" />; an
        /// unsuccessful <see cref="BinarySerializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> does not implement <see cref="T:System.Collections.Generic.IEnumerable{System.Collections.Generic.KeyValuePair`2}" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is MapSchema mapSchema)
            {
                if (GetDictionaryTypes(type) is (Type keyType, Type valueType))
                {
                    var pairType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
                    var collection = Expression.Variable(typeof(ICollection<>).MakeGenericType(pairType));
                    var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(pairType));
                    var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(pairType));
                    var loop = Expression.Label();

                    var toList = typeof(Enumerable)
                        .GetMethod(nameof(Enumerable.ToList))
                        .MakeGenericMethod(pairType);

                    var getCount = collection.Type
                        .GetProperty("Count");

                    var getEnumerator = enumerable.Type
                        .GetMethod("GetEnumerator", Type.EmptyTypes);

                    var getCurrent = enumerator.Type
                        .GetProperty(nameof(IEnumerator.Current));

                    var getKey = pairType
                        .GetProperty("Key")
                        .GetGetMethod();

                    var getValue = pairType
                        .GetProperty("Value")
                        .GetGetMethod();

                    var moveNext = typeof(IEnumerator)
                        .GetMethod(nameof(IEnumerator.MoveNext), Type.EmptyTypes);

                    var writeInteger = typeof(BinaryWriter)
                        .GetMethod(nameof(BinaryWriter.WriteInteger), new[] { typeof(long) });

                    var writeKey = SerializerBuilder
                        .BuildExpression(Expression.Property(Expression.Property(enumerator, getCurrent), getKey), new StringSchema(), context);

                    var writeValue = SerializerBuilder
                        .BuildExpression(Expression.Property(Expression.Property(enumerator, getCurrent), getValue), mapSchema.Value, context);

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
                        throw new UnsupportedTypeException(type, $"Failed to map {mapSchema} to {type}.", exception);
                    }

                    // var collection = value is ICollection<KeyValuePair<TKey, TValue>>
                    //     ? (ICollection<KeyValuePair<TKey, TValue>>)value
                    //     : (ICollection<KeyValuePair<TKey, TValue>>)value.ToList();
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
                                                Expression.Block(writeKey, writeValue),
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
                    return BinarySerializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryMapSerializerBuilderCase)} can only be applied to dictionary types."));
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryMapSerializerBuilderCase)} can only be applied to {nameof(MapSchema)}s."));
            }
        }
    }
}
