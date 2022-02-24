namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Implements a <see cref="SchemaBuilder" /> case that matches integral types less than or
    /// equal to 32 bits.
    /// </summary>
    public class IntSchemaBuilderCase : SchemaBuilderCase, ISchemaBuilderCase
    {
        /// <summary>
        /// Builds an <see cref="IntSchema" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="SchemaBuilderCaseResult" /> with an <see cref="IntSchema" />
        /// if <paramref name="type" /> is less than or equal to 32 bits; an unsuccessful
        /// <see cref="SchemaBuilderCaseResult" /> with an <see cref="UnsupportedTypeException" />
        /// otherwise.
        /// </returns>
        /// <inheritdoc />
        public virtual SchemaBuilderCaseResult BuildSchema(Type type, SchemaBuilderContext context)
        {
            if (
                type == typeof(sbyte) || type == typeof(byte) ||
                type == typeof(short) || type == typeof(ushort) || type == typeof(char) ||
                type == typeof(int) || type == typeof(uint))
            {
                var intSchema = new IntSchema();

                try
                {
                    context.Schemas.Add(type, intSchema);
                }
                catch (ArgumentException exception)
                {
                    throw new InvalidOperationException($"A schema for {type} already exists on the schema builder context.", exception);
                }

                return SchemaBuilderCaseResult.FromSchema(intSchema);
            }
            else
            {
                return SchemaBuilderCaseResult.FromException(new UnsupportedTypeException(type, $"{nameof(IntSchemaBuilderCase)} can only be applied to integral types less than or equal than 32 bits."));
            }
        }
    }
}
