namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="MapSchema" />
    /// and attempts to map it to dictionary types.
    /// </summary>
    public class BinaryMapDeserializerBuilderCase : MapDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryMapDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build key and value deserializers.
        /// </param>
        public BinaryMapDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build key and value deserializers.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="MapSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is a dictionary type and <paramref name="schema" /> is a <see cref="MapSchema" />; an
        /// unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" /> otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> is not assignable from any supported dictionary
        /// <see cref="Type" /> and does not have a constructor that can be used to instantiate it.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is MapSchema mapSchema)
            {
                if (GetDictionaryTypes(type) is (Type keyType, Type valueType))
                {
                    var instantiateDictionary = BuildIntermediateDictionary(type);

                    var readInteger = typeof(BinaryReader)
                        .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                    var readKey = DeserializerBuilder
                        .BuildExpression(keyType, new StringSchema(), context);

                    var readValue = DeserializerBuilder
                        .BuildExpression(valueType, mapSchema.Value, context);

                    var dictionary = Expression.Parameter(instantiateDictionary.Type);
                    var index = Expression.Variable(typeof(long));
                    var size = Expression.Variable(typeof(long));
                    var outer = Expression.Label();
                    var inner = Expression.Label();

                    var add = dictionary.Type
                        .GetMethod("Add", new[] { readKey.Type, readValue.Type });

                    // var dictionary = new ...;
                    //
                    // outer: while (true)
                    // {
                    //     var size = reader.ReadInteger();
                    //
                    //     // if the block is empty, the map is complete:
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
                    //         dictionary.Add(..., ...);
                    //     }
                    // }
                    //
                    // return dictionary;
                    Expression expression = Expression.Block(
                        new[] { dictionary, index, size },
                        Expression.Assign(dictionary, instantiateDictionary),
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
                                        Expression.Call(dictionary, add, readKey, readValue)),
                                    inner)),
                            outer),
                        dictionary);

                    if (!type.IsAssignableFrom(expression.Type) && GetDictionaryConstructor(type) is ConstructorInfo constructor)
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
                        throw new UnsupportedTypeException(type, $"Failed to map {mapSchema} to {type}.", exception);
                    }
                }
                else
                {
                    return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinaryMapDeserializerBuilderCase)} can only be applied to dictionary types."));
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryMapDeserializerBuilderCase)} can only be applied to {nameof(MapSchema)}s."));
            }
        }
    }
}
