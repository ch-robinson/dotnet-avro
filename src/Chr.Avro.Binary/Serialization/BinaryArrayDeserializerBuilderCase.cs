namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

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
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="resolution" />
        /// is an <see ref="ArrayResolution" /> and <paramref name="schema" /> is an <see cref="ArraySchema" />;
        /// an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the resolved <see cref="Type" /> is not assignable from any supported array
        /// or collection <see cref="Type" /> and does not have a constructor that can be used to
        /// instantiate it.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is ArraySchema arraySchema)
            {
                if (resolution is ArrayResolution arrayResolution)
                {
                    var instantiateCollection = BuildIntermediateCollection(arrayResolution);

                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    var readItem = DeserializerBuilder
                        .BuildExpression(arrayResolution.ItemType, arraySchema.Item, context);

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

                    if (!arrayResolution.Type.IsAssignableFrom(expression.Type) && FindEnumerableConstructor(arrayResolution) is ConstructorResolution constructorResolution)
                    {
                        expression = Expression.New(constructorResolution.Constructor, expression);
                    }

                    try
                    {
                        return BinaryDeserializerBuilderCaseResult.FromExpression(
                            BuildConversion(expression, arrayResolution.Type));
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new UnsupportedTypeException(resolution.Type, $"Failed to map {arraySchema} to {resolution.Type}.", exception);
                    }
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(BinaryArrayDeserializerBuilderCase)} can only be applied to {nameof(ArrayResolution)}s."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryArrayDeserializerBuilderCase)} can only be applied to {nameof(ArraySchema)}s."));
            }
        }
    }
}
