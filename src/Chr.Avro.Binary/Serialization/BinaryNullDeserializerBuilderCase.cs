namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="NullSchema" />.
    /// </summary>
    public class BinaryNullDeserializerBuilderCase : NullDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="NullSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="NullSchema" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(Type type, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is NullSchema)
            {
                return BinaryDeserializerBuilderCaseResult.FromExpression(
                    Expression.Default(type));
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryNullDeserializerBuilderCase)} can only be applied to {nameof(NullSchema)}s."));
            }
        }
    }
}
