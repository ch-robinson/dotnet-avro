namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
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
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is MapSchema mapSchema)
            {
                var dictionaryTypes = GetDictionaryTypes(type);

                if (dictionaryTypes is not null || type == typeof(object))
                {
                    // support dynamic mapping:
                    var keyType = dictionaryTypes?.Key ?? typeof(object);
                    var valueType = dictionaryTypes?.Value ?? typeof(object);

                    var pairType = typeof(KeyValuePair<,>).MakeGenericType(keyType, valueType);
                    var collection = Expression.Variable(typeof(ICollection<>).MakeGenericType(pairType));
                    var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(pairType));
                    var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(pairType));
                    var loop = Expression.Label();

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
                        .BuildExpression(Expression.Property(Expression.Property(enumerator, getCurrent), getKey), new StringSchema(), context, registerExpression);

                    var writeValue = SerializerBuilder
                        .BuildExpression(Expression.Property(Expression.Property(enumerator, getCurrent), getValue), mapSchema.Value, context, registerExpression);

                    var dispose = typeof(IDisposable)
                        .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

                    Expression expression;

                    try
                    {
                        expression = BuildConversion(value, collection.Type);
                    }
                    catch (Exception exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {mapSchema} to {type}.", exception);
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
