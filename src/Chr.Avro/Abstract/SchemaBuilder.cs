using Chr.Avro.Resolution;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Builds Avro schemas for .NET types.
    /// </summary>
    public interface ISchemaBuilder
    {
        /// <summary>
        /// Builds a schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to build a schema for.
        /// </typeparam>
        /// <param name="cache">
        /// An optional schema cache. The cache can be used to provide schemas for certain types,
        /// and it will also be populated as the schema is built.
        /// </param>
        /// <returns>
        /// A schema that matches the type.
        /// </returns>
        Schema BuildSchema<T>(IDictionary<Type, Schema> cache = null);

        /// <summary>
        /// Builds a schema.
        /// </summary>
        /// <param name="type">
        /// The type to build a schema for.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache can be used to provide schemas for certain types,
        /// and it will also be populated as the schema is built.
        /// </param>
        /// <returns>
        /// A schema that matches the type.
        /// </returns>
        Schema BuildSchema(Type type, IDictionary<Type, Schema> cache = null);
    }

    /// <summary>
    /// Builds Avro schemas for specific type resolutions. Used by <see cref="SchemaBuilder" /> to
    /// break apart schema building logic.
    /// </summary>
    public interface ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a schema for a type resolution. If the case does not apply to the provided
        /// resolution, this method should throw an exception.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to build a schema for.
        /// </param>
        /// <param name="cache">
        /// A schema cache. If a schema is cached for a specific type, that schema will be returned
        /// for all subsequent occurrences of the type.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="Schema" />.
        /// </returns>
        Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache);

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        bool IsMatch(TypeResolution resolution);
    }

    /// <summary>
    /// A schema builder configured with a reasonable set of default cases.
    /// </summary>
    public class SchemaBuilder : ISchemaBuilder
    {
        /// <summary>
        /// A list of cases that the schema builder will attempt to apply. If the first case fails,
        /// the schema builder will try the next case, and so on until all cases have been attempted.
        /// </summary>
        protected ICollection<ISchemaBuilderCase> Cases;

        /// <summary>
        /// A resolver to retrieve type information from.
        /// </summary>
        protected ITypeResolver Resolver;

        /// <summary>
        /// Creates a new schema builder.
        /// </summary>
        /// <param name="cases">
        /// An optional collection of cases. If provided, this collection will replace the default
        /// list.
        /// </param>
        /// <param name="typeResolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the schema
        /// builder will use the default <see cref="ReflectionResolver" />.
        /// </param>
        public SchemaBuilder(ICollection<ISchemaBuilderCase> cases = null, ITypeResolver typeResolver = null)
        {
            Resolver = typeResolver ?? new ReflectionResolver();

            Cases = cases ?? new List<ISchemaBuilderCase>()
            {
                new ArraySchemaBuilderCase(this),
                new BooleanSchemaBuilderCase(),
                new BytesSchemaBuilderCase(),
                new DecimalSchemaBuilderCase(),
                new DoubleSchemaBuilderCase(),
                new DurationSchemaBuilderCase(),
                new EnumSchemaBuilderCase(),
                new FloatSchemaBuilderCase(),
                new IntSchemaBuilderCase(),
                new LongSchemaBuilderCase(),
                new MapSchemaBuilderCase(this),
                new RecordSchemaBuilderCase(this),
                new StringSchemaBuilderCase(),
                new TimestampSchemaBuilderCase(),
                new UriSchemaBuilderCase(),
                new UuidSchemaBuilderCase()
            };
        }

        /// <summary>
        /// Builds a schema.
        /// </summary>
        /// <typeparam name="T">
        /// The type to build a schema for.
        /// </typeparam>
        /// <param name="cache">
        /// An optional schema cache. The cache can be used to provide schemas for certain types,
        /// and it will also be populated as the schema is built.
        /// </param>
        /// <returns>
        /// A schema that matches the type.
        /// </returns>
        public Schema BuildSchema<T>(IDictionary<Type, Schema> cache = null)
        {
            return BuildSchema(typeof(T), cache);
        }

        /// <summary>
        /// Builds a schema.
        /// </summary>
        /// <param name="type">
        /// The type to build a schema for.
        /// </param>
        /// <param name="cache">
        /// An optional schema cache. The cache can be used to provide schemas for certain types,
        /// and it will also be populated as the schema is built.
        /// </param>
        /// <returns>
        /// A schema that matches the type.
        /// </returns>
        public Schema BuildSchema(Type type, IDictionary<Type, Schema> cache = null)
        {
            if (cache == null)
            {
                cache = new Dictionary<Type, Schema>();
            }

            var resolution = Resolver.ResolveType(type);

            if (cache.TryGetValue(resolution.Type, out var existing))
            {
                return existing;
            }

            var match = Cases.FirstOrDefault(c => c.IsMatch(resolution));

            if (match == null)
            {
                throw new UnsupportedTypeException(type, $"No schema builder case could be applied to {resolution.Type.FullName} ({resolution.GetType().Name}).");
            }

            var schema = match.BuildSchema(resolution, cache);

            if (resolution.IsNullable)
            {
                return new UnionSchema(new Schema[] { new NullSchema(), schema });
            }

            return schema;
        }
    }

    /// <summary>
    /// A base <see cref="ISchemaBuilderCase" /> implementation.
    /// </summary>
    public abstract class SchemaBuilderCase : ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a schema for a type resolution. If the case does not apply to the provided
        /// resolution, this method should throw an exception.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to build a schema for.
        /// </param>
        /// <param name="cache">
        /// A schema cache. If a schema is cached for a specific type, that schema will be returned
        /// for all subsequent occurrences of the type.
        /// </param>
        /// <returns>
        /// A subclass of <see cref="Schema" />.
        /// </returns>
        public abstract Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache);

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        public abstract bool IsMatch(TypeResolution resolution);
    }

    /// <summary>
    /// A schema builder case that matches <see cref="ArrayResolution" />.
    /// </summary>
    public class ArraySchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// A schema builder instance that will be used to resolve array item types.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the schema builder is set to null.
        /// </exception>
        protected readonly ISchemaBuilder SchemaBuilder;

        /// <summary>
        /// Creates a new array schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve array item types.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the schema builder is null.
        /// </exception>
        public ArraySchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder is null.");
        }

        /// <summary>
        /// Builds an array schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// An <see cref="ArraySchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an <see cref="ArrayResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is ArrayResolution array))
            {
                throw new ArgumentException("The array case can only be applied to array resolutions.", nameof(resolution));
            }

            var schema = new ArraySchema(SchemaBuilder.BuildSchema(array.ItemType));
            cache.Add(array.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="ArrayResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is ArrayResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="BooleanResolution" />.
    /// </summary>
    public class BooleanSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a boolean schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="BooleanSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="BooleanResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is BooleanResolution boolean))
            {
                throw new ArgumentException("The boolean case can only be applied to boolean resolutions.", nameof(resolution));
            }
            
            var schema = new BooleanSchema();
            cache.Add(boolean.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="BooleanResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is BooleanResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="ByteArrayResolution" />.
    /// </summary>
    public class BytesSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a byte array schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="BytesSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="ByteArrayResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is ByteArrayResolution bytes))
            {
                throw new ArgumentException("The byte array case can only be applied to byte array resolutions.", nameof(resolution));
            }
            
            var schema = new BytesSchema();
            cache.Add(bytes.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="ByteArrayResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is ByteArrayResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="DecimalResolution" />.
    /// </summary>
    public class DecimalSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a decimal schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="BytesSchema" /> with a <see cref="DecimalLogicalType" /> that matches the
        /// type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="DecimalResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is DecimalResolution @decimal))
            {
                throw new ArgumentException("The decimal case can only be applied to decimal resolutions.", nameof(resolution));
            }
            
            var schema = new BytesSchema()
            {
                LogicalType = new DecimalLogicalType(@decimal.Precision, @decimal.Scale)
            };

            cache.Add(@decimal.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="DecimalResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is DecimalResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="FloatingPointResolution" /> (double-precision).
    /// </summary>
    public class DoubleSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a double schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="DoubleSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a 16-bit <see cref="FloatingPointResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is FloatingPointResolution @double) || @double.Size != 16)
            {
                throw new ArgumentException("The double case can only be applied to 16-bit floating point resolutions.", nameof(resolution));
            }

            var schema = new DoubleSchema();
            cache.Add(@double.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a 16-bit <see cref="FloatingPointResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is FloatingPointResolution @double && @double.Size == 16;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="DurationResolution" />.
    /// </summary>
    public class DurationSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a duration schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="StringSchema" />.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="DurationResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is DurationResolution duration))
            {
                throw new ArgumentException("The duration case can only be applied to duration resolutions.", nameof(resolution));
            }

            var schema = new StringSchema();
            cache.Add(duration.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="DurationResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is DurationResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="EnumResolution" />.
    /// </summary>
    public class EnumSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds an enum schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="EnumSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an <see cref="EnumResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is EnumResolution @enum))
            {
                throw new ArgumentException("The enum case can only be applied to enum resolutions.", nameof(resolution));
            }

            if (@enum.IsFlagEnum)
            {
                var schema = new LongSchema();
                cache.Add(@enum.Type, schema);

                return schema;
            }
            else
            {
                var name = @enum.Namespace == null
                    ? @enum.Name.Value
                    : $"{@enum.Namespace.Value}.{@enum.Name.Value}";

                var schema = new EnumSchema(name);
                cache.Add(@enum.Type, schema);

                foreach (var symbol in @enum.Symbols)
                {
                    schema.Symbols.Add(symbol.Name.Value);
                }

                return schema;
            }
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="EnumResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is EnumResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="FloatingPointResolution" /> (single-precision).
    /// </summary>
    public class FloatSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a float schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="FloatSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an 8-bit <see cref="FloatingPointResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is FloatingPointResolution @float) || @float.Size != 8)
            {
                throw new ArgumentException("The double case can only be applied to 8-bit floating point resolutions.", nameof(resolution));
            }

            var schema = new FloatSchema();
            cache.Add(@float.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an 8-bit <see cref="FloatingPointResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is FloatingPointResolution @float && @float.Size == 8;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="IntegerResolution" /> (32-bit and smaller).
    /// </summary>
    public class IntSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds an int schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// An <see cref="IntSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an <see cref="IntegerResolution" /> or specifies a
        /// size greater than 32 bits.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is IntegerResolution @int) || @int.Size > 32)
            {
                throw new ArgumentException("The int case can only be applied to 32-bit or smaller integer resolutions.", nameof(resolution));
            }

            var schema = new IntSchema();
            cache.Add(@int.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="IntegerResolution" /> with a size less than or
        /// equal to 32 bits.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is IntegerResolution @int && @int.Size <= 32;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="IntegerResolution" /> (larger than 32-bit).
    /// </summary>
    public class LongSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a long schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="LongSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an <see cref="IntegerResolution" /> or specifies a
        /// size less than or equal to 32 bits.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is IntegerResolution @long) || @long.Size <= 32)
            {
                throw new ArgumentException("The long case can only be applied to integer resolutions larger than 32 bits.", nameof(resolution));
            }

            var schema = new LongSchema();
            cache.Add(@long.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is an <see cref="IntegerResolution" /> with a size greater than
        /// 32 bits.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is IntegerResolution @long && @long.Size > 32;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="MapResolution" />.
    /// </summary>
    public class MapSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// A schema builder instance that will be used to resolve map value types.
        /// </summary>
        protected readonly ISchemaBuilder SchemaBuilder;

        /// <summary>
        /// Creates a new map schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve map value types.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the schema builder is null.
        /// </exception>
        public MapSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Builds a map schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="MapSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an <see cref="MapResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is MapResolution map))
            {
                throw new ArgumentException("The map case can only be applied to map resolutions.", nameof(resolution));
            }

            var schema = new MapSchema(SchemaBuilder.BuildSchema(map.ValueType));
            cache.Add(map.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="MapResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is MapResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="RecordResolution" />.
    /// </summary>
    public class RecordSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// A schema builder instance that will be used to resolve record field types.
        /// </summary>
        protected readonly ISchemaBuilder SchemaBuilder;

        /// <summary>
        /// Creates a new record schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve record field types.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the schema builder is null.
        /// </exception>
        public RecordSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder ?? throw new ArgumentNullException(nameof(schemaBuilder), "Schema builder cannot be null.");
        }

        /// <summary>
        /// Builds a record schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="RecordSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not an <see cref="RecordResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is RecordResolution record))
            {
                throw new ArgumentException("The record case can only be applied to record resolutions.", nameof(resolution));
            }
            
            var name = record.Namespace == null
                ? record.Name.Value
                : $"{record.Namespace.Value}.{record.Name.Value}";

            var schema = new RecordSchema(name);
            cache.Add(record.Type, schema);

            foreach (var field in record.Fields)
            {
                schema.Fields.Add(new RecordField(field.Name.Value, SchemaBuilder.BuildSchema(field.Type, cache)));
            }

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="RecordResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is RecordResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="StringResolution" />.
    /// </summary>
    public class StringSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a string schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="StringSchema" /> that matches the type resolution.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="StringResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is StringResolution @string))
            {
                throw new ArgumentException("The string case can only be applied to string resolutions.", nameof(resolution));
            }

            var schema = new StringSchema();
            cache.Add(@string.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="StringResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is StringResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="TimestampResolution" />.
    /// </summary>
    public class TimestampSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a timestamp schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="StringSchema" />.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="TimestampResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is TimestampResolution timestamp))
            {
                throw new ArgumentException("The timestamp case can only be applied to timestamp resolutions.", nameof(resolution));
            }

            var schema = new StringSchema();
            cache.Add(timestamp.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="TimestampResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is TimestampResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="UriResolution" />.
    /// </summary>
    public class UriSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a URI schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="StringSchema" />.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="UriResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is UriResolution uri))
            {
                throw new ArgumentException("The URI case can only be applied to URI resolutions.", nameof(resolution));
            }

            var schema = new StringSchema();
            cache.Add(uri.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="UriResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is UriResolution;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="UuidResolution" />.
    /// </summary>
    public class UuidSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Builds a UUID schema.
        /// </summary>
        /// <param name="resolution">
        /// A type resolution.
        /// </param>
        /// <param name="cache">
        /// A schema cache.
        /// </param>
        /// <returns>
        /// A <see cref="StringSchema" />.
        /// </returns>
        /// <exception cref="ArgumentException">
        /// Thrown when the resolution is not a <see cref="UuidResolution" />.
        /// </exception>
        public override Schema BuildSchema(TypeResolution resolution, IDictionary<Type, Schema> cache)
        {
            if (!(resolution is UuidResolution uuid))
            {
                throw new ArgumentException("The UUID case can only be applied to UUID resolutions.", nameof(resolution));
            }

            var schema = new StringSchema();
            cache.Add(uuid.Type, schema);

            return schema;
        }

        /// <summary>
        /// Determines whether the case can be applied to a resolution.
        /// </summary>
        /// <returns>
        /// Whether the resolution is a <see cref="UuidResolution" />.
        /// </returns>
        public override bool IsMatch(TypeResolution resolution)
        {
            return resolution is UuidResolution;
        }
    }
}
