namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="IntegerResolution" />
    /// (32-bit and smaller).
    /// </summary>
    public class IntSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds an <see cref="IntSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="IntSchema" />
        /// if <paramref name="resolution" /> is an <see cref="IntegerResolution" /> with size
        /// <c>32</c> or less; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is IntegerResolution integerResolution)
            {
                var intSchema = new IntSchema();

                if (integerResolution.Size <= 32)
                {
                    try
                    {
                        context.Schemas.Add(integerResolution.Type.GetUnderlyingType(), intSchema);
                    }
                    catch (ArgumentException exception)
                    {
                        throw new InvalidOperationException($"A schema for {integerResolution.Type} already exists on the schema builder context.", exception);
                    }

                    return SchemaBuilderCaseResult.FromSchema(intSchema);
                }
                else
                {
                    return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(IntSchemaBuilderCase)} can only be applied to {nameof(IntegerResolution)}s with size 32 or less."));
                }
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(IntSchemaBuilderCase)} can only be applied to {nameof(IntegerResolution)}s."));
            }
        }
    }
}
