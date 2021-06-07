namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="FloatSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryFloatDeserializerBuilderCase : FloatDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="FloatSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="FloatSchema" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="float" /> cannot be converted to <paramref name="type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is FloatSchema floatSchema)
            {
                var readSingle = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadSingle), Type.EmptyTypes);

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, readSingle), type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {floatSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryFloatDeserializerBuilderCase)} can only be applied to {nameof(FloatSchema)}s."));
            }
        }
    }
}
