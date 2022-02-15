namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="FixedSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryFixedSerializerBuilderCase : FixedSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for an <see cref="FixedSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> <paramref name="schema" />
        /// is a <see cref="FixedSchema" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="T:System.Byte[]" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema is FixedSchema fixedSchema)
            {
                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(byte[]));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {fixedSchema} to {type}.", exception);
                }

                var bytes = Expression.Parameter(typeof(byte[]));

                var exceptionConstructor = typeof(OverflowException)
                    .GetConstructor(new[] { typeof(string) });

                var writeFixed = typeof(BinaryWriter)
                    .GetMethod(nameof(BinaryWriter.WriteFixed), new[] { typeof(byte[]) });

                return BinarySerializerBuilderCaseResult.FromExpression(
                    Expression.Block(
                        new[] { bytes },
                        Expression.Assign(bytes, expression),
                        Expression.IfThen(
                            Expression.NotEqual(Expression.ArrayLength(bytes), Expression.Constant(fixedSchema.Size)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant($"Only arrays of size {fixedSchema.Size} can be serialized to {fixedSchema}.")))),
                        Expression.Call(context.Writer, writeFixed, bytes)));
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryFixedSerializerBuilderCase)} can only be applied to {nameof(FixedSchema)}s."));
            }
        }
    }
}
