namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="IntegerResolution" />
    /// (larger than 32-bit).
    /// </summary>
    public class LongSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="LongSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="LongSchema" />
        /// if <paramref name="resolution" /> is an <see cref="IntegerResolution" /> with size
        /// greater than<c>32</c>; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is IntegerResolution integerResolution)
            {
                var longSchema = new LongSchema();

                if (integerResolution.Size > 32)
                {
                    try
                    {
                        context.Schemas.Add(integerResolution.Type.GetUnderlyingType(), longSchema);
                    }
                    catch (ArgumentException exception)
                    {
                        throw new InvalidOperationException($"A schema for {integerResolution.Type} already exists on the schema builder context.", exception);
                    }

                    return SchemaBuilderCaseResult.FromSchema(longSchema);
                }
                else
                {
                    return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(IntSchemaBuilderCase)} can only be applied to {nameof(IntegerResolution)}s with size greater than 32."));
                }
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(IntSchemaBuilderCase)} can only be applied to {nameof(IntegerResolution)}s."));
            }
        }
    }
}
