namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="DoubleSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryDoubleSerializerBuilderCase : DoubleSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="DoubleSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="DoubleSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="double" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is DoubleSchema doubleSchema)
            {
                var writeDouble = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteDouble), new[] { typeof(double) });

                try
                {
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeDouble, BuildConversion(value, typeof(double))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {doubleSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDoubleSerializerBuilderCase)} can only be applied to {nameof(DoubleSchema)}s."));
            }
        }
    }
}
