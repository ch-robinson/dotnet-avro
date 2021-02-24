namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="BinaryDeserializerBuilder" /> case that matches <see cref="BooleanSchema" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryBooleanDeserializerBuilderCase : BooleanDeserializerBuilderCase, IBinaryDeserializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinaryDeserializer{T}" /> for a <see cref="BooleanSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinaryDeserializerBuilderCaseResult" /> if <paramref name="schema" />
        /// is a <see cref="BooleanSchema" />; an unsuccessful <see cref="BinaryDeserializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <see cref="bool" /> cannot be converted to the resolved <see cref="Type" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinaryDeserializerBuilderCaseResult BuildExpression(TypeResolution resolution, Schema schema, BinaryDeserializerBuilderContext context)
        {
            if (schema is BooleanSchema booleanSchema)
            {
                var readBoolean = typeof(BinaryReader)
                    .GetMethod(nameof(BinaryReader.ReadBoolean), Type.EmptyTypes);

                try
                {
                    return BinaryDeserializerBuilderCaseResult.FromExpression(
                        BuildConversion(Expression.Call(context.Reader, readBoolean), resolution.Type));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"Failed to map {booleanSchema} to {resolution.Type}.", exception);
                }
            }
            else
            {
                return BinaryDeserializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryBooleanDeserializerBuilderCase)} can only be applied to {nameof(BooleanSchema)}s."));
            }
        }
    }
}
