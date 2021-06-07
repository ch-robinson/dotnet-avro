namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="ArraySchema" />
    /// and attempts to map it to enumerable types.
    /// </summary>
    public class BinaryArrayDeserializerBuilderCase : ArrayDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryArrayDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build item deserializers.
        /// </param>
        public BinaryArrayDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build item deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for an <see cref="ArraySchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is an enumerable type and <paramref name="schema" /> is an <see cref="ArraySchema" />;
        /// an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> is not assignable from any supported array or
        /// collection <see cref="Type" /> and does not have a constructor that can be used to
        /// instantiate it.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                if (GetEnumerableType(type) is Type itemType)
                {
                    var instantiateCollection = BuildIntermediateCollection(type);

                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    var readItem = DeserializerBuilder
                        .BuildExpression(itemType, arraySchema.Item, context);

                    var collection = Expression.Parameter(instantiateCollection.Type);
                    var index = Expression.Variable(typeof(long));
                    var size = Expression.Variable(typeof(long));
                    var outer = Expression.Label();
                    var inner = Expression.Label();

                    var add = collection.Type
                        .GetMethod("Add", new[] { readItem.Type });

                    // var collection = new List<T>();
                    //
                    // outer: while (true)
                    // {
                    //     var size = reader.ReadInteger();
                    //
                    //     // if the block is empty, the array is complete:
                    //     if (size == 0L) break outer;
                    //
                    //     // if the block size is negative, the number of bytes in the block
                    //     // follows, so read and discard:
                    //     if (size < 0L)
                    //     {
                    //          size *= -1L;
                    //          reader.ReadInteger();
                    //     }
                    //
                    //     var index = 0;
                    //
                    //     inner: while (true)
                    //     {
                    //         // primitive for:
                    //         if (index++ == size) break inner;
                    //
                    //         collection.Add(...);
                    //     }
                    // }
                    //
                    // return collection;
                    Expression expression = Expression.Block(
                        new[] { collection, index, size },
                        Expression.Assign(collection, instantiateCollection),
                        Expression.Loop(
                            Expression.Block(
                                Expression.Assign(size, Expression.Call(context.Reader, readInteger)),
                                Expression.IfThen(
                                    Expression.Equal(size, Expression.Constant(0L)),
                                    Expression.Break(outer)),
                                Expression.IfThen(
                                    Expression.LessThan(size, Expression.Constant(0L)),
                                    Expression.Block(
                                        Expression.MultiplyAssign(size, Expression.Constant(-1L)),
                                        Expression.Call(context.Reader, readInteger))),
                                Expression.Assign(index, Expression.Constant(0L)),
                                Expression.Loop(
                                    Expression.Block(
                                        Expression.IfThen(
                                            Expression.Equal(Expression.PostIncrementAssign(index), size),
                                            Expression.Break(inner)),
                                        Expression.Call(collection, add, readItem)),
                                    inner)),
                            outer),
                        collection);

                    if (!type.IsAssignableFrom(expression.Type) && GetCollectionConstructor(type) is ConstructorInfo constructor)
                    {
                        expression = Expression.New(constructor, expression);
                    }

                    try
                    {
                        return BinaryDeserializerBuilderCaseResult.FromExpression(
                            BuildConversion(expression, type));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(type, $"Failed to map {arraySchema} to {type}.", exception);
                    }
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryArrayDeserializerBuilderCase)} can only be applied to enumerable types."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryArrayDeserializerBuilderCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
