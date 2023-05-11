namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="FloatSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryFloatSerializerBuilderCase : FloatSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="FloatSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="FloatSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="float" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context, bool registerExpression)
        {
            if (schema is FloatSchema floatSchema)
            {
                var writeSingle = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteSingle), new[] { typeof(float) });

                try
                {
                    return BinarySerializerBuilderCaseResult.FromExpression(
                        Expression.Call(context.Writer, writeSingle, BuildConversion(value, typeof(float))));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {floatSchema} to {type}.", exception);
                }
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryFloatSerializerBuilderCase)} can only be applied to {nameof(FloatSchema)}s."));
            }
        }
    }
}
