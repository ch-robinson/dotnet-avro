namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches <see cref="bool" />.
    /// </summary>
    public class BooleanSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BooleanSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with a <see cref="BooleanSchema" />
        /// if <paramref name="type" /> is <see cref="bool" />; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (type == typeof(bool))
            {
                var booleanSchema = new BooleanSchema();

                try
                {
                    context.Schemas.Add(type, booleanSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(booleanSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(BooleanSchemaBuilderCase)} can only be applied to the {typeof(bool)} type."));
            }
        }
    }
}
