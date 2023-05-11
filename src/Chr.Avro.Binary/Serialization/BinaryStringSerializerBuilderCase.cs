namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="StringSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryStringSerializerBuilderCase : StringSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="StringSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="StringSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="string" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is StringSchema stringSchema)
            {
                var writeString = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteString), new[] { typeof(string) });

                try
                {
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeString, BuildConversion(value, typeof(string))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {stringSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryStringSerializerBuilderCase)} can only be applied to {nameof(StringSchema)}s."));
            }
        }
    }
}
