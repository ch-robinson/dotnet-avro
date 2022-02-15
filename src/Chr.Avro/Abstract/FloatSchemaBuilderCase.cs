namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="float" />.
    /// </summary>
    public class FloatSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="FloatSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="FloatSchema" />
        /// if <paramref name="type" /> is a <see cref="float" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(float))
            {
                var floatSchema = new FloatSchema();

                try
                {
                    context.Schemas.Add(type, floatSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(floatSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(FloatSchemaBuilderCase)} can only be applied to the {typeof(float)} type."));
            }
        }
    }
}
