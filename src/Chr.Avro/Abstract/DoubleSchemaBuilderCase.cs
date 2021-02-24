namespace Chr.Avro.Abstract
{
    using System;
    using Chr.Avro.Infrastructure;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="FloatingPointResolution" />
    /// (double-precision).
    /// </summary>
    public class DoubleSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="DoubleSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="DoubleSchema" />
        /// if <paramref name="resolution" /> is a <see cref="FloatingPointResolution" /> with
        /// size <c>16</c>; an unsuccessful <see cref="SchemaBuilderCaseResult" /> with an
        /// <see cref="UnsupportedTypeException" /> otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(TypeResolution resolution, SchemaBuilderContext context)
        {
            if (resolution is FloatingPointResolution floatingPointResolution)
            {
                if (floatingPointResolution.Size == 16)
                {
                    var doubleSchema = new DoubleSchema();

                    try
                    {
                        context.Schemas.Add(floatingPointResolution.Type.GetUnderlyingType(), doubleSchema);
                    }
                    catch (ArgumentException exception)
                    {
                        throw new InvalidOperationException($"A schema for {floatingPointResolution.Type} already exists on the schema builder context.", exception);
                    }

                    return SchemaBuilderCaseResult.FromSchema(doubleSchema);
                }
                else
                {
                    return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(DoubleSchemaBuilderCase)} can only be applied to {nameof(FloatingPointResolution)}s with size 16."));
                }
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(resolution.Type, $"{nameof(DoubleSchemaBuilderCase)} can only be applied to {nameof(FloatingPointResolution)}s."));
            }
        }
    }
}
