using Chr.Avro.Resolution;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

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
        Schema BuildSchema<T>(ConcurrentDictionary<Type, Schema>? cache = null);

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
        Schema BuildSchema(Type type, ConcurrentDictionary<Type, Schema>? cache = null);
    }

    /// <summary>
    /// Represents the outcome of a schema builder case.
    /// </summary>
    public interface ISchemaBuildResult
    {
        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Schema" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        ICollection<Exception> Exceptions { get; }

        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        Schema? Schema { get; }
    }

    /// <summary>
    /// Builds Avro schemas for specific types. See <see cref="SchemaBuilder" /> for implementation
    /// details.
    /// </summary>
    public interface ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a schema for a type resolution.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to build a schema for.
        /// </param>
        /// <param name="cache">
        /// A schema cache. If a schema is cached for a type, that same schema instance will be
        /// returned for all occurrences of the type.
        /// </param>
        /// <returns>
        /// A build result.
        /// </returns>
        ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache);
    }

    /// <summary>
    /// A schema builder configured with a reasonable set of default cases.
    /// </summary>
    public class SchemaBuilder : ISchemaBuilder
    {
        /// <summary>
        /// A list of cases that the schema builder will attempt to apply. If the first case does
        /// not match, the schema builder will try the next case, and so on until all cases have
        /// been tested.
        /// </summary>
        public IEnumerable<ISchemaBuilderCase> Cases { get; }

        /// <summary>
        /// A resolver to retrieve type information from.
        /// </summary>
        public ITypeResolver Resolver { get; }

        /// <summary>
        /// Creates a new schema builder.
        /// </summary>
        /// <param name="temporalBehavior">
        /// Whether the builder should build string schemas (ISO 8601) or long schemas (timestamp
        /// logical types) for timestamp resolutions.
        /// </param>
        /// <param name="typeResolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the schema
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public SchemaBuilder(TemporalBehavior temporalBehavior = TemporalBehavior.Iso8601, ITypeResolver? typeResolver = null)
            : this(CreateCaseBuilders(temporalBehavior), typeResolver) { }

        /// <summary>
        /// Creates a new schema builder.
        /// </summary>
        /// <param name="caseBuilders">
        /// A list of case builders.
        /// </param>
        /// <param name="typeResolver">
        /// A resolver to retrieve type information from. If no resolver is provided, the schema
        /// builder will use the default <see cref="DataContractResolver" />.
        /// </param>
        public SchemaBuilder(IEnumerable<Func<ISchemaBuilder, ISchemaBuilderCase>> caseBuilders, ITypeResolver? typeResolver = null)
        {
            var cases = new List<ISchemaBuilderCase>();

            Cases = cases;
            Resolver = typeResolver ?? new DataContractResolver();

            // initialize cases last so that the schema builder is fully ready:
            foreach (var builder in caseBuilders)
            {
                cases.Add(builder(this));
            }
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
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case matches the type.
        /// </exception>
        public Schema BuildSchema<T>(ConcurrentDictionary<Type, Schema>? cache = null)
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
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when no case matches the type.
        /// </exception>
        public Schema BuildSchema(Type type, ConcurrentDictionary<Type, Schema>? cache = null)
        {
            if (cache == null)
            {
                cache = new ConcurrentDictionary<Type, Schema>();
            }

            var resolution = Resolver.ResolveType(type);

            if (!cache.TryGetValue(resolution.Type, out var schema))
            {
                var exceptions = new List<Exception>();

                foreach (var @case in Cases)
                {
                    var result = @case.BuildSchema(resolution, cache);

                    if (result.Schema != null)
                    {
                        schema = result.Schema;
                        break;
                    }

                    exceptions.AddRange(result.Exceptions);
                }

                if (schema == null)
                {
                    throw new UnsupportedTypeException(resolution.Type, $"No schema builder case could be applied to {resolution.Type.FullName} ({resolution.GetType().Name}).", new AggregateException(exceptions));
                }
            }

            if (resolution.IsNullable)
            {
                return new UnionSchema(new Schema[] { new NullSchema(), schema });
            }

            return schema;
        }

        /// <summary>
        /// Creates a default list of case builders.
        /// </summary>
        public static IEnumerable<Func<ISchemaBuilder, ISchemaBuilderCase>> CreateCaseBuilders(TemporalBehavior temporalBehavior)
        {
            return new Func<ISchemaBuilder, ISchemaBuilderCase>[]
            {
                builder => new ArraySchemaBuilderCase(builder),
                builder => new BooleanSchemaBuilderCase(),
                builder => new BytesSchemaBuilderCase(),
                builder => new DecimalSchemaBuilderCase(),
                builder => new DoubleSchemaBuilderCase(),
                builder => new DurationSchemaBuilderCase(),
                builder => new EnumSchemaBuilderCase(builder),
                builder => new FloatSchemaBuilderCase(),
                builder => new IntSchemaBuilderCase(),
                builder => new LongSchemaBuilderCase(),
                builder => new MapSchemaBuilderCase(builder),
                builder => new RecordSchemaBuilderCase(builder),
                builder => new StringSchemaBuilderCase(),
                builder => new TimestampSchemaBuilderCase(temporalBehavior),
                builder => new UriSchemaBuilderCase(),
                builder => new UuidSchemaBuilderCase()
            };
        }
    }

    /// <summary>
    /// A base <see cref="ISchemaBuildResult" /> implementation.
    /// </summary>
    public class SchemaBuildResult : ISchemaBuildResult
    {
        /// <summary>
        /// Any exceptions related to the applicability of the case. If <see cref="Schema" /> is
        /// not null, these exceptions should be interpreted as warnings.
        /// </summary>
        public ICollection<Exception> Exceptions { get; set; } = new List<Exception>();

        /// <summary>
        /// The result of applying the case. If null, the case was not applied successfully.
        /// </summary>
        public Schema? Schema { get; set; }
    }

    /// <summary>
    /// A base <see cref="ISchemaBuilderCase" /> implementation.
    /// </summary>
    public abstract class SchemaBuilderCase : ISchemaBuilderCase
    {
        /// <summary>
        /// Builds a schema for a type resolution.
        /// </summary>
        /// <param name="resolution">
        /// The resolution to build a schema for.
        /// </param>
        /// <param name="cache">
        /// A schema cache. If a schema is cached for a specific type, that schema will be returned
        /// for all subsequent occurrences of the type.
        /// </param>
        /// <returns>
        /// A build result.
        /// </returns>
        public abstract ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache);
    }

    /// <summary>
    /// A schema builder case that matches <see cref="ArrayResolution" />.
    /// </summary>
    public class ArraySchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// A schema builder instance that will be used to resolve array item types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Creates a new array schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve array item types.
        /// </param>
        public ArraySchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder;
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
        /// A successful <see cref="ArraySchema" /> build result if <paramref name="resolution" />
        /// is an <see cref="ArrayResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is ArrayResolution array)
            {
                result.Schema = cache.GetOrAdd(array.Type, _ => new ArraySchema(SchemaBuilder.BuildSchema(array.ItemType, cache)));
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="BooleanSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="BooleanResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is BooleanResolution boolean)
            {
                result.Schema = cache.GetOrAdd(boolean.Type, _ => new BooleanSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="BytesSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="ByteArrayResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is ByteArrayResolution bytes)
            {
                result.Schema = cache.GetOrAdd(bytes.Type, _ => new BytesSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="BytesSchema" />/<see cref="DecimalLogicalType" /> build result
        /// if <paramref name="resolution" /> is a <see cref="DecimalResolution" />; an unsuccessful
        /// <see cref="UnsupportedTypeException" /> build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is DecimalResolution @decimal)
            {
                result.Schema = cache.GetOrAdd(@decimal.Type, _ => new BytesSchema()
                {
                    LogicalType = new DecimalLogicalType(@decimal.Precision, @decimal.Scale)
                });
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="DoubleSchema" /> build result if <paramref name="resolution" />
        /// is a double-precision <see cref="FloatingPointResolution" />; an unsuccessful
        /// <see cref="UnsupportedTypeException" /> build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is FloatingPointResolution @double && @double.Size == 16)
            {
                result.Schema = cache.GetOrAdd(@double.Type, _ => new DoubleSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="StringSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="DurationResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is DurationResolution duration)
            {
                result.Schema = cache.GetOrAdd(duration.Type, _ => new StringSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="EnumResolution" />.
    /// </summary>
    public class EnumSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// A schema builder instance that will be used to resolve underlying integral types.
        /// </summary>
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Creates a new enum schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve underlying integral types.
        /// </param>
        public EnumSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder;
        }

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
        /// A successful <see cref="EnumSchema" /> build result if <paramref name="resolution" />
        /// is an <see cref="EnumResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is EnumResolution @enum)
            {
                result.Schema = cache.GetOrAdd(@enum.Type, _ =>
                {
                    if (@enum.IsFlagEnum)
                    {
                        return SchemaBuilder.BuildSchema(@enum.UnderlyingType, cache);
                    }
                    else
                    {
                        var name = @enum.Namespace == null
                            ? @enum.Name.Value
                            : $"{@enum.Namespace.Value}.{@enum.Name.Value}";

                        var schema = new EnumSchema(name);

                        foreach (var symbol in @enum.Symbols)
                        {
                            schema.Symbols.Add(symbol.Name.Value);
                        }

                        return schema;
                    }
                });
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="FloatSchema" /> build result if <paramref name="resolution" />
        /// is a single-precision <see cref="FloatingPointResolution" />; an unsuccessful
        /// <see cref="UnsupportedTypeException" /> build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is FloatingPointResolution @float && @float.Size == 8)
            {
                result.Schema = cache.GetOrAdd(@float.Type, _ => new FloatSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="IntSchema" /> build result if <paramref name="resolution" />
        /// is an <see cref="IntegerResolution" /> with <see cref="IntegerResolution.Size" /> less
        /// than or equal to 32; an unsuccessful <see cref="UnsupportedTypeException" /> build
        /// result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is IntegerResolution @int && @int.Size <= 32)
            {
                result.Schema = cache.GetOrAdd(@int.Type, _ => new IntSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="IntSchema" /> build result if <paramref name="resolution" />
        /// is an <see cref="IntegerResolution" /> with <see cref="IntegerResolution.Size" />
        /// greater than 32; an unsuccessful <see cref="UnsupportedTypeException" /> build result
        /// otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is IntegerResolution @long && @long.Size > 32)
            {
                result.Schema = cache.GetOrAdd(@long.Type, _ => new LongSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Creates a new map schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve map value types.
        /// </param>
        public MapSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder;
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
        /// A successful <see cref="MapSchema" /> build result if <paramref name="resolution" /> is
        /// a <see cref="MapResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is MapResolution map)
            {
                result.Schema = cache.GetOrAdd(map.Type, _ => new MapSchema(SchemaBuilder.BuildSchema(map.ValueType, cache)));
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        public ISchemaBuilder SchemaBuilder { get; }

        /// <summary>
        /// Creates a new record schema builder case.
        /// </summary>
        /// <param name="schemaBuilder">
        /// A schema builder instance that will be used to resolve record field types.
        /// </param>
        public RecordSchemaBuilderCase(ISchemaBuilder schemaBuilder)
        {
            SchemaBuilder = schemaBuilder;
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
        /// A successful <see cref="RecordSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="RecordResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is RecordResolution record)
            {
                if (cache.TryGetValue(record.Type, out var schema))
                {
                    result.Schema = schema;
                }
                else
                {
                    var name = record.Namespace == null
                        ? record.Name.Value
                        : $"{record.Namespace.Value}.{record.Name.Value}";

                    var instance = new RecordSchema(name);

                    if (!cache.TryAdd(record.Type, instance))
                    {
                        throw new InvalidOperationException("Failed to cache record schema prior to building its fields.");
                    }

                    foreach (var field in record.Fields)
                    {
                        instance.Fields.Add(new RecordField(field.Name.Value, SchemaBuilder.BuildSchema(field.Type, cache)));
                    }

                    result.Schema = instance;
                }
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="StringSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="StringResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is StringResolution @string)
            {
                result.Schema = cache.GetOrAdd(@string.Type, _ => new StringSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
        }
    }

    /// <summary>
    /// A schema builder case that matches <see cref="TimestampResolution" />.
    /// </summary>
    public class TimestampSchemaBuilderCase : SchemaBuilderCase
    {
        /// <summary>
        /// Whether the case should build string schemas (ISO 8601) or long schemas (timestamp
        /// logical types).
        /// </summary>
        public TemporalBehavior TemporalBehavior { get; }

        /// <summary>
        /// Creates a new timestamp schema builder case.
        /// </summary>
        /// <param name="temporalBehavior">
        /// Whether the case should build string schemas (ISO 8601) or long schemas (timestamp
        /// logical types).
        /// </param>
        public TimestampSchemaBuilderCase(TemporalBehavior temporalBehavior)
        {
            TemporalBehavior = temporalBehavior;
        }

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
        /// A successful <see cref="StringSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="TimestampResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is TimestampResolution timestamp)
            {
                result.Schema = cache.GetOrAdd(timestamp.Type, _ => TemporalBehavior switch
                {
                    TemporalBehavior.EpochMicroseconds => new LongSchema()
                    {
                        LogicalType = new MicrosecondTimestampLogicalType()
                    },
                    TemporalBehavior.EpochMilliseconds => new LongSchema()
                    {
                        LogicalType = new MillisecondTimestampLogicalType()
                    },
                    TemporalBehavior.Iso8601 => new StringSchema(),
                    _ => throw new ArgumentOutOfRangeException(nameof(TemporalBehavior))
                });
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="StringSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="UriResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is UriResolution uri)
            {
                result.Schema = cache.GetOrAdd(uri.Type, _ => new StringSchema());
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
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
        /// A successful <see cref="StringSchema" /> build result if <paramref name="resolution" />
        /// is a <see cref="UuidResolution" />; an unsuccessful <see cref="UnsupportedTypeException" />
        /// build result otherwise.
        /// </returns>
        public override ISchemaBuildResult BuildSchema(TypeResolution resolution, ConcurrentDictionary<Type, Schema> cache)
        {
            var result = new SchemaBuildResult();

            if (resolution is UuidResolution uuid)
            {
                result.Schema = cache.GetOrAdd(uuid.Type, new StringSchema()
                {
                    LogicalType = new UuidLogicalType()
                });
            }
            else
            {
                result.Exceptions.Add(new UnsupportedTypeException(resolution.Type));
            }

            return result;
        }
    }
}
