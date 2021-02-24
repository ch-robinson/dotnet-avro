namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="DoubleSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryDoubleDeserializerBuilderCase : DoubleDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="DoubleSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="DoubleSchema" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="double" /> cannot be converted to the resolved <see cref="Type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is DoubleSchema doubleSchema)
            {
                var readDouble = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadDouble), Type.EmptyTypes);

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, readDouble), resolution.Type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"Failed to map {doubleSchema} to {resolution.Type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDoubleDeserializerBuilderCase)} can only be applied to {nameof(DoubleSchema)}s."));
            }
        }
    }
}
