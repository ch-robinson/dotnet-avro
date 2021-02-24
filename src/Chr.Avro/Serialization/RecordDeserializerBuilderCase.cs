namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Chr.Avro.Abstract;
    using Chr.Avro.Resolution;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="RecordSchema" />.
    /// </summary>
    public abstract class RecordDeserializerBuilderCase
    {
        /// <summary>
        /// Gets a type that can be used to deserialize missing record fields.
        /// </summary>
        /// <param name="schema">
        /// A schema to select a compatible type for.
        /// </param>
        /// <returns>
        /// <see cref="IEnumerable{T}" /> if the schema is an array schema (or a union schema
        /// containing only array/null schemas); <see cref="IDictionary{TKey, TValue}" /> if the
        /// schema is a map schema (or a union schema containing only map/null schemas);
        /// <see cref="object" /> otherwise.
        /// </returns>
        protected virtual Type GetSurrogateType(Schema schema)
        {
            var schemas = schema is UnionSchema union
                ? union.Schemas
                : new[] { schema };

            if (schemas.All(s => s is ArraySchema || s is NullSchema))
            {
                var items = schemas.OfType<ArraySchema>()
                    .Select(a => a.Item)
                    .Distinct()
                    .ToList();

                return typeof(IEnumerable<>).MakeGenericType(GetSurrogateType(
                    items.Count > 1
                        ? new UnionSchema(items)
                        : items.SingleOrDefault()));
            }
            else if (schemas.All(s => s is MapSchema || s is NullSchema))
            {
                var values = schemas.OfType<MapSchema>()
                    .Select(m => m.Value)
                    .Distinct()
                    .ToList();

                return typeof(IDictionary<,>).MakeGenericType(typeof(string), GetSurrogateType(
                    values.Count > 1
                        ? new UnionSchema(values)
                        : values.SingleOrDefault()));
            }

            return typeof(object);
        }

        /// <summary>
        /// Gets a constructor with a matching parameter for each <see cref="RecordField" /> in a
        /// <see cref="RecordSchema" />.
        /// </summary>
        /// <param name="resolution">
        /// A <see cref="RecordResolution" /> containing information about the record
        /// <see cref="Type" />.
        /// </param>
        /// <param name="schema">
        /// A <see cref="RecordSchema" /> to match to <paramref name="resolution" />.
        /// </param>
        /// <returns>
        /// A <see cref="ConstructorResolution" /> from <paramref name="resolution" /> if one
        /// matches; <c>null</c> otherwise.
        /// </returns>
        protected ConstructorResolution? FindRecordConstructor(RecordResolution resolution, RecordSchema schema)
        {
            return resolution.Constructors.FirstOrDefault(constructor =>
                schema.Fields.All(field =>
                    constructor.Parameters.Any(parameter =>
                        parameter.Name.IsMatch(field.Name))));
        }
    }
}
