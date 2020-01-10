using Chr.Avro.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Chr.Avro.Abstract
{
    /// <summary>
    /// A common base for Avro schemas and related models (such as record fields).
    /// </summary>
    public abstract class Declaration
    {
        /// <summary>
        /// A regular expression describing a legal Avro name.
        /// </summary>
        /// <remarks>
        /// See the <a href="https://avro.apache.org/docs/current/spec.html#names">Avro spec</a>
        /// for the full naming rules.
        /// </remarks>
        protected static readonly Regex AllowedName = new Regex(@"^[A-Za-z_][A-Za-z0-9_]*$");
    }

    /// <summary>
    /// An Avro schema. Represented in JSON by a string (naming a defined type), an object with a
    /// "type" key, or an array (representing a union).
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#schemas">Avro spec</a> for
    /// details.
    /// </remarks>
    public abstract class Schema : Declaration
    {
        /// <summary>
        /// The schema’s logical type.
        /// </summary>
        public LogicalType? LogicalType { get; set; }
    }

    /// <summary>
    /// An Avro schema representing a complex type.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#schema_complex">Avro spec</a>
    /// for details.
    /// </remarks>
    public abstract class ComplexSchema : Schema { }

    /// <summary>
    /// An Avro schema representing an array. Arrays contain a variable number of items of a single
    /// type.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Arrays">Avro spec</a> for
    /// details.
    /// </remarks>
    public class ArraySchema : ComplexSchema
    {
        private Schema item = null!;

        /// <summary>
        /// The schema of the items in the array.
        /// </summary>
        public Schema Item
        {
            get
            {
                return item ?? throw new InvalidOperationException();
            }
            set
            {
                item = value ?? throw new ArgumentNullException(nameof(value), "Item schema cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new array schema.
        /// </summary>
        /// <param name="item">
        /// The schema of the items in the array.
        /// </param>
        public ArraySchema(Schema item)
        {
            Item = item;
        }
    }

    /// <summary>
    /// An Avro schema representing a map of string keys to values.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Maps">Avro spec</a> for
    /// details.
    /// </remarks>
    public class MapSchema : ComplexSchema
    {
        private Schema value = null!;

        /// <summary>
        /// The schema of the values in the map.
        /// </summary>
        public Schema Value
        {
            get
            {
                return value ?? throw new InvalidOperationException();
            }
            set
            {
                this.value = value ?? throw new ArgumentNullException(nameof(value), "Value schema cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new map schema.
        /// </summary>
        /// <param name="value">
        /// The schema of the values in the map.
        /// </param>
        public MapSchema(Schema value)
        {
            Value = value;
        }
    }

    /// <summary>
    /// An Avro schema representing a union of schemas.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Unions">Avro spec</a> for
    /// details.
    /// </remarks>
    public class UnionSchema : ComplexSchema
    {
        private ConstrainedSet<Schema> schemas = null!;

        /// <summary>
        /// The union members.
        /// </summary>
        /// <remarks>
        /// This collection will enforce schema type constraints, namely that union schemas may not
        /// contain other union schemas or multiple schemas of the same type. Duplicate name
        /// constraints, however, are not enforced.
        /// </remarks>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema of the same type already exists as a member of the union.
        /// </exception>
        public ICollection<Schema> Schemas
        {
            get
            {
                return schemas ?? throw new InvalidOperationException();
            }
            set
            {
                schemas = value?.ToConstrainedSet((schema, set) =>
                {
                    if (schema == null)
                    {
                        throw new ArgumentNullException(nameof(value), "A union member cannot be null.");
                    }

                    if (schema is UnionSchema)
                    {
                        throw new InvalidSchemaException("A union may not immediately contain another union.");
                    }

                    if (!(schema is NamedSchema) && set.Any(m => m.GetType() == schema.GetType()))
                    {
                        throw new InvalidSchemaException("A union may not contain more than one schema of the same type.");
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Union schema collection may not be null.");
            }
        }

        /// <summary>
        /// Creates a new union schema.
        /// </summary>
        /// <param name="schemas">
        /// The union members.
        /// </param>
        /// <exception cref="InvalidSchemaException">
        /// Thrown when a schema of the same type already exists as a member of the union.
        /// </exception>
        public UnionSchema(ICollection<Schema>? schemas = null)
        {
            Schemas = schemas ?? new List<Schema>();
        }
    }

    /// <summary>
    /// An Avro schema identified by a name and (optionally) aliases. The name and aliases may each
    /// be qualified by a namespace.
    /// </summary>
    public abstract class NamedSchema : ComplexSchema
    {
        private ConstrainedSet<string> aliases = null!;

        private string name = null!;

        /// <summary>
        /// The alternate names by which the schema can be identified.
        /// </summary>
        /// <remarks>
        /// Aliases are fully-qualified; they do not inherit the schema’s namespace. Duplicate
        /// aliases will be filtered from the collection.
        /// </remarks>
        /// <exception cref="InvalidNameException">
        /// Thrown when an alias does not conform to the Avro specification.
        /// </exception>
        public ICollection<string> Aliases
        {
            get
            {
                return aliases ?? throw new InvalidOperationException();
            }
            set
            {
                aliases = value?.ToConstrainedSet((alias, set) =>
                {
                    if (alias == null)
                    {
                        throw new ArgumentNullException(nameof(value), "A schema alias cannot be null.");
                    }

                    if (!alias.Split('.').All(c => AllowedName.Match(c).Success))
                    {
                        throw new InvalidNameException(alias);
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Schema alias collection cannot be null.");
            }
        }

        /// <summary>
        /// The qualified schema name.
        /// </summary>
        /// <exception cref="InvalidNameException">
        /// Thrown when the name is set to a value that does not conform to the Avro naming rules.
        /// </exception>
        public string FullName
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value), "Schema name cannot be null.");
                }

                if (!value.Split('.').All(c => AllowedName.Match(c).Success))
                {
                    throw new InvalidNameException(value);
                }

                name = value;
            }
        }

        /// <summary>
        /// The unqualified schema name.
        /// </summary>
        /// <remarks>
        /// Setting this property to a qualified name will update the full name. Setting this
        /// property to an unqualified name will retain the existing namespace.
        /// </remarks>
        /// <exception cref="InvalidNameException">
        /// Thrown when the name is set to a value that does not conform to the Avro naming rules.
        /// </exception>
        public string Name
        {
            get
            {
                return FullName.Substring(FullName.LastIndexOf('.') + 1);
            }
            set
            {
                FullName = Namespace != null && value?.IndexOf('.') < 0
                    ? $"{Namespace}.{value}"
                    : value!;
            }
        }

        /// <summary>
        /// The schema namespace.
        /// </summary>
        /// <remarks>
        /// This property will return null if no namespace is set. Setting this property to null
        /// or the empty string will clear the namespace.
        /// </remarks>
        /// <exception cref="InvalidNameException">
        /// Thrown when the namespace is set to a value that does not conform to the Avro naming
        /// rules.
        /// </exception>
        public string? Namespace
        {
            get
            {
                var index = FullName.LastIndexOf('.');

                return index < 0
                    ? null
                    : FullName.Substring(0, index);
            }
            set
            {
                value = string.IsNullOrEmpty(value)
                    ? string.Empty
                    : $"{value}.";

                FullName = $"{value}{Name}";
            }
        }

        /// <summary>
        /// Creates a new named schema.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro naming rules.
        /// </exception>
        public NamedSchema(string name)
        {
            Aliases = new List<string>();
            FullName = name;
        }
    }

    /// <summary>
    /// An Avro schema representing a set of string constants (symbols). All
    /// symbols in an enum must be unique.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Enums">Avro spec</a> for
    /// details.
    /// </remarks>
    public class EnumSchema : NamedSchema
    {
        private ConstrainedSet<string> symbols = null!;

        /// <summary>
        /// A human-readable description of the enum.
        /// </summary>
        public string? Documentation { get; set; }

        /// <summary>
        /// The enum symbols.
        /// </summary>
        /// <exception cref="InvalidSymbolException">
        /// Thrown when a symbol does not conform to the Avro specification.
        /// </exception>
        public ICollection<string> Symbols
        {
            get
            {
                return symbols ?? throw new InvalidOperationException();
            }
            set
            {
                symbols = value?.ToConstrainedSet((symbol, set) =>
                {
                    if (symbol == null)
                    {
                        throw new ArgumentNullException(nameof(value), "An enum symbol cannot be null.");
                    }

                    if (!AllowedName.Match(symbol).Success)
                    {
                        throw new InvalidSymbolException(symbol);
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Enum symbol collection cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new enum schema.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <param name="symbols">
        /// The enum symbols.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro naming rules.
        /// </exception>
        /// <exception cref="InvalidSymbolException">
        /// Thrown when a symbol does not conform to the Avro naming rules.
        /// </exception>
        public EnumSchema(string name, ICollection<string>? symbols = null) : base(name)
        {
            Symbols = symbols ?? new List<string>();
        }
    }

    /// <summary>
    /// An Avro schema representing a fixed number of bytes.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Fixed">Avro spec</a> for
    /// details.
    /// </remarks>
    public class FixedSchema : NamedSchema
    {
        private int size;

        /// <summary>
        /// The number of bytes per value.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when size is set to a negative value.
        /// </exception>
        public int Size
        {
            get
            {
                return size;
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException("A fixed schema cannot have a negative size.");
                }

                size = value;
            }
        }

        /// <summary>
        /// Creates a new fixed schema.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <param name="size">
        /// The number of bytes per value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the size is negative.
        /// </exception>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro specification.
        /// </exception>
        public FixedSchema(string name, int size) : base(name)
        {
            Size = size;
        }
    }

    /// <summary>
    /// An Avro schema representing a record (a data structure with a fixed number of fields).
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#schema_record">Avro spec</a>
    /// for details.
    /// </remarks>
    public class RecordSchema : NamedSchema
    {
        private ConstrainedSet<RecordField> fields = null!;

        /// <summary>
        /// A human-readable description of the record.
        /// </summary>
        public string? Documentation { get; set; }

        /// <summary>
        /// The record fields.
        /// </summary>
        /// <remarks>
        /// Avro doesn’t allow duplicate field names, but that constraint isn’t enforced here—the
        /// onus is on the user to ensure that the record schema is valid.
        /// </remarks>
        public ICollection<RecordField> Fields
        {
            get
            {
                return fields ?? throw new InvalidOperationException();
            }
            set
            {
                fields = value?.ToConstrainedSet((field, set) =>
                {
                    if (field == null)
                    {
                        throw new ArgumentNullException(nameof(value), "A record field cannot be null.");
                    }

                    return true;
                }) ?? throw new ArgumentNullException(nameof(value), "Record field collection cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new record schema.
        /// </summary>
        /// <param name="name">
        /// The qualified schema name.
        /// </param>
        /// <param name="fields">
        /// The record fields.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the schema name does not conform to the Avro specification.
        /// </exception>
        public RecordSchema(string name, ICollection<RecordField>? fields = null) : base(name)
        {
            Fields = fields ?? new List<RecordField>();
        }
    }

    /// <summary>
    /// A field in an Avro record schema.
    /// </summary>
    public sealed class RecordField : Declaration
    {
        private string name = null!;

        private Schema type = null!;

        /// <summary>
        /// A human-readable description of the field.
        /// </summary>
        public string? Documentation { get; set; }

        /// <summary>
        /// The name of the field.
        /// </summary>
        /// <exception cref="InvalidNameException">
        /// Thrown when the name is set to a value that does not conform to the Avro naming rules.
        /// </exception>
        public string Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(value), "Field name cannot be null.");
                }

                if (!AllowedName.Match(value).Success)
                {
                    throw new InvalidNameException(value);
                }

                name = value;
            }
        }

        /// <summary>
        /// The type of the field.
        /// </summary>
        public Schema Type
        {
            get
            {
                return type ?? throw new InvalidOperationException();
            }
            set
            {
                type = value ?? throw new ArgumentNullException(nameof(value), "Field type cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new record field.
        /// </summary>
        /// <param name="name">
        /// The field name.
        /// </param>
        /// <param name="type">
        /// The field type.
        /// </param>
        /// <exception cref="InvalidNameException">
        /// Thrown when the field name does not conform to the Avro naming rules.
        /// </exception>
        public RecordField(string name, Schema type)
        {
            Name = name;
            Type = type;
        }
    }

    /// <summary>
    /// An Avro schema representing a primitive type.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#schema_primitive">Avro spec</a>
    /// for details.
    /// </remarks>
    public abstract class PrimitiveSchema : Schema { }

    /// <summary>
    /// An Avro schema representing a boolean value.
    /// </summary>
    public class BooleanSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing a variable-length sequence of bytes.
    /// </summary>
    public class BytesSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing a double-precision (64-bit) floating-point number.
    /// </summary>
    public class DoubleSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing a single-precision (32-bit) floating-point number.
    /// </summary>
    public class FloatSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing a 32-bit signed integer.
    /// </summary>
    public class IntSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing a 64-bit signed integer.
    /// </summary>
    public class LongSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing an absent value.
    /// </summary>
    public class NullSchema : PrimitiveSchema { }

    /// <summary>
    /// An Avro schema representing a Unicode character sequence.
    /// </summary>
    public class StringSchema : PrimitiveSchema { }
}
