namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="TimeSpan" />.
    /// </summary>
    public class DurationSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="StringSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="StringSchema" />
        /// if <paramref name="type" /> is <see cref="TimeSpan" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(TimeSpan))
            {
                var durationSchema = new StringSchema();

                try
                {
                    context.Schemas.Add(type, durationSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(durationSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(DurationSchemaBuilderCase)} can only be applied to the {nameof(TimeSpan)} type."));
            }
        }
    }
}
