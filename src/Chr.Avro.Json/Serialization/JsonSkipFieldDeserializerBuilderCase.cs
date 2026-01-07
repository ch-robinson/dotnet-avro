namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="JsonDeserializerBuilder" /> case that skips over JSON-encoded
    /// fields without deserializing them.
    /// </summary>
    internal class JsonSkipFieldDeserializerBuilderCase : DeserializerBuilderCase, IJsonDeserializerBuilderCase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSkipFieldDeserializerBuilderCase" /> class.
        /// </summary>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance that will be used to build expressions for skipping nested structures.
        /// </param>
        public JsonSkipFieldDeserializerBuilderCase(IJsonDeserializerBuilder deserializerBuilder)
        {
            DeserializerBuilder = deserializerBuilder ?? throw new ArgumentNullException(nameof(deserializerBuilder), "JSON deserializer builder cannot be null.");
        }

        /// <summary>
        /// Gets the deserializer builder instance that will be used to build expressions for skipping nested structures.
        /// </summary>
        public IJsonDeserializerBuilder DeserializerBuilder { get; }

        /// <summary>
        /// Builds a <see cref="JsonDeserializer{T}" /> that skips the field.
        /// </summary>
        /// <returns>
        /// A successful <see cref="JsonDeserializerBuilderCaseResult" /> if <paramref name="type" />
        /// is <see cref="SkipField" />; an unsuccessful <see cref="JsonDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual JsonDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, JsonDeserializerBuilderContext context)
        {
            if (type != typeof(SkipField))
            {
                return JsonDeserializerBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(JsonSkipFieldDeserializerBuilderCase)} only supports {nameof(SkipField)}."));
            }

            try
            {
                return JsonDeserializerBuilderCaseResult.FromExpression(BuildSkipExpression(schema, context));
            }
            catch (InvalidOperationException exception)
            {
                throw new UnsupportedSchemaException(schema, $"Unable to skip {schema}.", exception);
            }
        }

        private Expression BuildSkipExpression(Schema schema, JsonDeserializerBuilderContext context)
        {
            return schema switch
            {
                NullSchema => SkipValue(context),
                BooleanSchema => SkipValue(context),
                IntSchema or LongSchema => SkipValue(context),
                FloatSchema => SkipValue(context),
                DoubleSchema => SkipValue(context),
                BytesSchema => SkipValue(context),
                StringSchema => SkipValue(context),
                FixedSchema => SkipValue(context),
                RecordSchema recordSchema => SkipRecord(context, recordSchema),
                EnumSchema => SkipValue(context),
                ArraySchema arraySchema => SkipArray(context, arraySchema),
                MapSchema mapSchema => SkipMap(context, mapSchema),
                UnionSchema unionSchema => SkipUnion(context, unionSchema),
                _ => throw new InvalidOperationException($"Unknown schema type: {schema.GetType().Name}"),
            };
        }

        private Expression SkipValue(JsonDeserializerBuilderContext context)
        {
            var skip = typeof(Utf8JsonReader).GetMethod(nameof(Utf8JsonReader.Skip))!;
            return Expression.Call(context.Reader, skip);
        }

        private Expression SkipRecord(JsonDeserializerBuilderContext context, RecordSchema recordSchema)
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

        private Expression SkipArray(JsonDeserializerBuilderContext context, ArraySchema arraySchema)
        {
            var read = typeof(Utf8JsonReader).GetMethod(nameof(Utf8JsonReader.Read))!;
            var getTokenType = typeof(Utf8JsonReader).GetProperty(nameof(Utf8JsonReader.TokenType))!;

            var skipItem = DeserializerBuilder.BuildExpression(typeof(SkipField), arraySchema.Item, context);

            var loopLabel = Expression.Label();

            // while (reader.Read())
            // {
            //     if (reader.TokenType == JsonTokenType.EndArray)
            //         break;
            //     skipItem();
            // }
            return Expression.Loop(
                Expression.Block(
                    Expression.IfThen(
                        Expression.Not(Expression.Call(context.Reader, read)),
                        Expression.Break(loopLabel)),
                    Expression.IfThen(
                        Expression.Equal(
                            Expression.Property(context.Reader, getTokenType),
                            Expression.Constant(JsonTokenType.EndArray)),
                        Expression.Break(loopLabel)),
                    skipItem),
                loopLabel);
        }

        private Expression SkipMap(JsonDeserializerBuilderContext context, MapSchema mapSchema)
        {
            var read = typeof(Utf8JsonReader).GetMethod(nameof(Utf8JsonReader.Read))!;
            var getTokenType = typeof(Utf8JsonReader).GetProperty(nameof(Utf8JsonReader.TokenType))!;

            var skipValue = DeserializerBuilder.BuildExpression(typeof(SkipField), mapSchema.Value, context);

            var loopLabel = Expression.Label();

            // while (reader.Read())
            // {
            //     if (reader.TokenType == JsonTokenType.EndObject)
            //         break;
            //     reader.Read(); // skip the value (we already read the key)
            //     skipValue();
            // }
            return Expression.Loop(
                Expression.Block(
                    Expression.IfThen(
                        Expression.Not(Expression.Call(context.Reader, read)),
                        Expression.Break(loopLabel)),
                    Expression.IfThen(
                        Expression.Equal(
                            Expression.Property(context.Reader, getTokenType),
                            Expression.Constant(JsonTokenType.EndObject)),
                        Expression.Break(loopLabel)),
                    Expression.Call(context.Reader, read),
                    skipValue),
                loopLabel);
        }

        private Expression SkipUnion(JsonDeserializerBuilderContext context, UnionSchema unionSchema)
        {
            var read = typeof(Utf8JsonReader).GetMethod(nameof(Utf8JsonReader.Read))!;
            var getTokenType = typeof(Utf8JsonReader).GetProperty(nameof(Utf8JsonReader.TokenType))!;

            // For unions, we need to determine which schema is encoded
            // In JSON, unions are typically encoded as objects with a single key (the schema name)
            // or as the value directly if it's nullable
            var cases = unionSchema.Schemas.Select((schema, i) =>
                Expression.SwitchCase(
                    DeserializerBuilder.BuildExpression(typeof(SkipField), schema, context),
                    Expression.Constant(i)))
                .ToList();

            if (cases.Count == 0)
            {
                return SkipValue(context);
            }

            // Simple approach: skip the value directly
            return SkipValue(context);
        }
    }
}
