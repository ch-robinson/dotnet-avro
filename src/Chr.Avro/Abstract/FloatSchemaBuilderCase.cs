namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="FloatingPointResolution" />
    /// (single-precision).
    /// </summary>
    public class FloatSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="FloatSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="FloatSchema" />
        /// if <paramref name="resolution" /> is a <see cref="FloatingPointResolution" /> with
        /// size <c>8</c>; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is FloatingPointResolution floatingPointResolution)
            {
                if (floatingPointResolution.Size == 8)
                {
                    var floatSchema = new FloatSchema();

                    try
                    {
                        context.Schemas.Add(floatingPointResolution.Type.GetUnderlyingType(), floatSchema);
                    }
                    catch (ArgumentException exception)
                    {
                        throw new InvalidOperationException($"A schema for {floatingPointResolution.Type} already exists on the schema builder context.", exception);
                    }

                    return SchemaBuilderCaseResult.FromSchema(floatSchema);
                }
                else
                {
                    return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(FloatSchemaBuilderCase)} can only be applied to {nameof(FloatingPointResolution)}s with size 8."));
                }
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(FloatSchemaBuilderCase)} can only be applied to {nameof(FloatingPointResolution)}s."));
            }
        }
    }
}
