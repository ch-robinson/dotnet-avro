namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that skips over binary-encoded
    /// fields without deserializing them.
    /// </summary>
    internal class BinarySkipFieldDeserializerBuilderCase : DeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinarySkipFieldDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build expressions for skipping nested structures.
        /// </param>
        public BinarySkipFieldDeserializerBuilderCase(IBinaryDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "Binary deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build expressions for skipping nested structures.
        /// </summary>
        public IBinaryDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> that skips the field.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is <see cref="SkipField" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (type != typeof(SkipField))
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BinarySkipFieldDeserializerBuilderCase)} only supports {nameof(SkipField)}."));
            }

            try
            {
                return BinaryDeserializerBuilderCaseResult.FromExpression(BuildSkipExpression(schema, context));
            }
            catch (InvalidOperationException exception)
            {
                throw new UnsupportedSchemaException(schema, $"Unable to skip {schema}.", exception);
            }
        }

        private Expression BuildSkipExpression(Schema schema, BinaryDeserializerBuilderContext context)
        {
            return schema switch
            {
                NullSchema => Expression.Empty(),
                BooleanSchema => SkipBytes(context, 1),
                IntSchema or LongSchema => ReadInteger(context),
                FloatSchema => SkipBytes(context, 4),
                DoubleSchema => SkipBytes(context, 8),
                BytesSchema => SkipBytes(context, ReadInteger(context)),
                StringSchema => SkipBytes(context, ReadInteger(context)),
                FixedSchema fixedSchema => SkipBytes(context, fixedSchema.Size),
                RecordSchema recordSchema => SkipRecord(context, recordSchema),
                EnumSchema => ReadInteger(context),
                ArraySchema arraySchema => SkipArray(context, arraySchema),
                MapSchema mapSchema => SkipMap(context, mapSchema),
                UnionSchema unionSchema => SkipUnion(context, unionSchema),
                _ => throw new InvalidOperationException($"Unknown schema type: {schema.GetType().Name}"),
            };
        }

        private Expression ReadInteger(BinaryDeserializerBuilderContext context)
        {
            var readInteger = typeof(BinaryReader).GetMethod(nameof(BinaryReader.ReadInteger))!;
            return Expression.Call(context.Reader, readInteger);
        }

        private Expression SkipBytes(BinaryDeserializerBuilderContext context, int count)
        {
            var readFixedSpan = typeof(BinaryReader).GetMethod(nameof(BinaryReader.ReadFixedSpan))!;
            return Expression.Call(context.Reader, readFixedSpan, Expression.Constant(count));
        }

        private Expression SkipBytes(BinaryDeserializerBuilderContext context, Expression lengthExpression)
        {
            var readFixedSpan = typeof(BinaryReader).GetMethod(nameof(BinaryReader.ReadFixedSpan))!;
            return Expression.Call(context.Reader, readFixedSpan, Expression.Convert(lengthExpression, typeof(int)));
        }

        private Expression SkipRecord(BinaryDeserializerBuilderContext context, RecordSchema recordSchema)
        {
            var expressions = recordSchema.Fields
                .Select(field => DeserializerBuilder.BuildExpression(typeof(SkipField), field.Type, context))
                .ToList();

            if (expressions.Count == 0)
            {
                return Expression.Empty();
            }

            return Expression.Block(expressions);
        }

        private Expression SkipArray(BinaryDeserializerBuilderContext context, ArraySchema arraySchema)
        {
            var readInteger = typeof(BinaryReader).GetMethod(nameof(BinaryReader.ReadInteger))!;

            var skipItem = DeserializerBuilder.BuildExpression(typeof(SkipField), arraySchema.Item, context);

            var size = Expression.Variable(typeof(long));
            var index = Expression.Variable(typeof(long));
            var outer = Expression.Label();
            var inner = Expression.Label();

            // outer: while (true)
            // {
            //     var size = reader.ReadInteger();
            //
            //     if (size == 0L) break outer;
            //
            //     if (size < 0L)
            //     {
            //         size *= -1L;
            //         reader.ReadInteger();
            //     }
            //
            //     var index = 0L;
            //
            //     inner: while (true)
            //     {
            //         if (index++ == size) break inner;
            //         skipItem();
            //     }
            // }
            return Expression.Block(
                new[] { size, index },
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
                                skipItem),
                            inner)),
                    outer));
        }

        private Expression SkipMap(BinaryDeserializerBuilderContext context, MapSchema mapSchema)
        {
            var readInteger = typeof(BinaryReader).GetMethod(nameof(BinaryReader.ReadInteger))!;

            var skipKey = DeserializerBuilder.BuildExpression(typeof(SkipField), new StringSchema(), context);
            var skipValue = DeserializerBuilder.BuildExpression(typeof(SkipField), mapSchema.Value, context);

            var size = Expression.Variable(typeof(long));
            var index = Expression.Variable(typeof(long));
            var outer = Expression.Label();
            var inner = Expression.Label();

            // outer: while (true)
            // {
            //     var size = reader.ReadInteger();
            //
            //     if (size == 0L) break outer;
            //
            //     if (size < 0L)
            //     {
            //         size *= -1L;
            //         reader.ReadInteger();
            //     }
            //
            //     var index = 0L;
            //
            //     inner: while (true)
            //     {
            //         if (index++ == size) break inner;
            //         skipKey();
            //         skipValue();
            //     }
            // }
            return Expression.Block(
                new[] { size, index },
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
                                skipKey,
                                skipValue),
                            inner)),
                    outer));
        }

        private Expression SkipUnion(BinaryDeserializerBuilderContext context, UnionSchema unionSchema)
        {
            var position = typeof(BinaryReader).GetProperty(nameof(BinaryReader.Index))!;
            var readInteger = typeof(BinaryReader).GetMethod(nameof(BinaryReader.ReadInteger))!;
            var exceptionConstructor = typeof(InvalidEncodingException)
                .GetConstructor(new[] { typeof(long), typeof(string), typeof(Exception) })!;

            var cases = unionSchema.Schemas.Select((schema, i) =>
            {
                var skipExpression = DeserializerBuilder.BuildExpression(typeof(SkipField), schema, context);

                // Convert expression to void by wrapping in an expression block that discards the result
                Expression voidExpression;
                if (skipExpression.Type == typeof(void))
                {
                    voidExpression = skipExpression;
                }
                else
                {
                    voidExpression = Expression.Block(typeof(void), skipExpression);
                }

                return Expression.SwitchCase(voidExpression, Expression.Constant((long)i));
            }).ToArray();

            var index = Expression.Variable(typeof(long));
            return Expression.Block(
                new[] { index },
                Expression.Assign(index, Expression.Call(context.Reader, readInteger)),
                Expression.Switch(
                    index,
                    Expression.Throw(
                        Expression.New(
                            exceptionConstructor,
                            Expression.Property(context.Reader, position),
                            Expression.Constant($"Invalid union index; expected a value in [0-{unionSchema.Schemas.Count}). This may indicate invalid encoding earlier in the stream."),
                            Expression.Constant(null, typeof(Exception))),
                        typeof(void)),
                    cases));
        }
    }
}
