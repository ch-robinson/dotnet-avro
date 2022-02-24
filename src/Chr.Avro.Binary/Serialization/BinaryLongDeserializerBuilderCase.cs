namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="LongSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryLongDeserializerBuilderCase : LongDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="LongSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="LongSchema" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="long" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is LongSchema longSchema)
            {
                var readInteger = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadInteger), Type.EmptyTypes);

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, readInteger), type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {longSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryLongDeserializerBuilderCase)} can only be applied to {nameof(LongSchema)}s."));
            }
        }
    }
}
