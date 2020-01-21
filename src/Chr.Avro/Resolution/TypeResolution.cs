using System;
using System.Collections.Generic;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// Contains resolved information about a .NET type.
    /// </summary>
    /// <remarks>
    /// The type resolution framework is a light abstraction around the .NET type system. It allows
    /// most Chr.Avro behaviors to be customized and keeps reflection logic out of components like
    /// the schema builder.
    /// </remarks>
    public abstract class TypeResolution
    {
        private Type type = null!;

        /// <summary>
        /// Whether the resolved type can have a null value.
        /// </summary>
        public virtual bool IsNullable { get; set; }

        /// <summary>
        /// The resolved type.
        /// </summary>
        public virtual Type Type
        {
            get
            {
                return type ?? throw new InvalidOperationException();
            }
            set
            {
                type = value ?? throw new ArgumentNullException(nameof(value), "Resolved type cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new type resolution.
        /// </summary>
        /// <param name="type">
        /// The resolved type.
        /// </param>
        /// <param name="isNullable">
        /// Whether the resolved type can have a null value.
        /// </param>
        public TypeResolution(Type type, bool isNullable = false)
        {
            IsNullable = isNullable;
            Type = type;
        }
    }

    /// <summary>
    /// Contains resolved information about an array type.
    /// </summary>
    public class ArrayResolution : TypeResolution
    {
        private ICollection<ConstructorResolution> constructors = null!;

        private Type itemType = null!;

        /// <summary>
        /// The array constructors.
        /// </summary>
        public virtual ICollection<ConstructorResolution> Constructors
        {
            get
            {
                return constructors ?? throw new InvalidOperationException();
            }
            set
            {
                constructors = value ?? throw new ArgumentNullException(nameof(value), "Resolved constructor collection cannot be null.");
            }
        }

        /// <summary>
        /// The array item type.
        /// </summary>
        public virtual Type ItemType
        {
            get
            {
                return itemType ?? throw new InvalidOperationException();
            }
            set
            {
                itemType = value ?? throw new ArgumentNullException(nameof(value), "Resolved item type cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new array resolution.
        /// </summary>
        /// <param name="type">
        /// The array type.
        /// </param>
        /// <param name="itemType">
        /// The array item type.
        /// </param>
        /// <param name="constructors">
        /// The array constructors.
        /// </param>
        /// <param name="isNullable">
        /// Whether the array type can have a null value.
        /// </param>
        public ArrayResolution(Type type, Type itemType, ICollection<ConstructorResolution>? constructors = null, bool isNullable = false) : base(type, isNullable)
        {
            Constructors = constructors ?? new List<ConstructorResolution>();
            ItemType = itemType;
        }
    }

    /// <summary>
    /// Contains resolved information about a boolean type.
    /// </summary>
    public class BooleanResolution : TypeResolution
    {
        /// <summary>
        /// Creates a new boolean resolution.
        /// </summary>
        /// <param name="type">
        /// The boolean type.
        /// </param>
        /// <param name="isNullable">
        /// Whether the boolean type can have a null value.
        /// </param>
        public BooleanResolution(Type type, bool isNullable = false) : base(type, isNullable) { }
    }

    /// <summary>
    /// Contains resolved information about a byte array type.
    /// </summary>
    public class ByteArrayResolution : TypeResolution
    {
        /// <summary>
        /// Creates a new byte array resolution.
        /// </summary>
        /// <param name="type">
        /// The byte array type.
        /// </param>
        /// <param name="isNullable">
        /// Whether the byte array type can have a null value.
        /// </param>
        public ByteArrayResolution(Type type, bool isNullable = false) : base(type, isNullable) { }
    }

    /// <summary>
    /// Contains resolved information about a decimal type.
    /// </summary>
    public class DecimalResolution : TypeResolution
    {
        private int precision;

        private int scale;

        /// <summary>
        /// The number of digits.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is set to a value less than one or less than the scale.
        /// </exception>
        public virtual int Precision
        {
            get
            {
                return precision;
            }
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be greater than zero.");
                }

                if (value < Scale)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be greater than scale.");
                }

                precision = value;
            }
        }

        /// <summary>
        /// The number of digits to the right of the decimal point.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the scale is set to a value less than zero or greater than the precision.
        /// </exception>
        public virtual int Scale
        {
            get
            {
                return scale;
            }
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Scale must be a positive integer.");
                }

                if (value > Precision)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Scale must be less than or equal to precision.");
                }

                scale = value;
            }
        }

        /// <summary>
        /// Creates a new decimal resolution.
        /// </summary>
        /// <param name="type">
        /// The decimal type.
        /// </param>
        /// <param name="precision">
        /// The number of digits.
        /// </param>
        /// <param name="scale">
        /// The number of digits to the right of the decimal point.
        /// </param>
        /// <param name="isNullable">
        /// Whether the decimal type can have a null value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is less than one or less than the scale or the scale is less
        /// than zero or greater than the precision.
        /// </exception>
        public DecimalResolution(Type type, int precision, int scale, bool isNullable = false) : base(type, isNullable)
        {
            Precision = precision;
            Scale = scale;
        }
    }

    /// <summary>
    /// Contains resolved information about a duration type.
    /// </summary>
    public class DurationResolution : TypeResolution
    {
        private decimal precision;

        /// <summary>
        /// The precision of the duration type relative to 1 second. For example, millisecond
        /// precision = 0.001.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is set to a value less than zero.
        /// </exception>
        public virtual decimal Precision
        {
            get
            {
                return precision;
            }
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be a positive factor.");
                }

                precision = value;
            }
        }

        /// <summary>
        /// Creates a new duration resolution.
        /// </summary>
        /// <param name="type">
        /// The duration type.
        /// </param>
        /// <param name="precision">
        /// The precision of the duration type relative to 1 second. For example, millisecond
        /// precision = 0.001.
        /// </param>
        /// <param name="isNullable">
        /// Whether the duration type can have a null value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is less than zero.
        /// </exception>
        public DurationResolution(Type type, decimal precision, bool isNullable = false) : base(type, isNullable)
        {
            Precision = precision;
        }
    }

    /// <summary>
    /// Contains resolved information about an IEEE 754 floating-point number type.
    /// </summary>
    public class FloatingPointResolution : TypeResolution
    {
        private int size;

        /// <summary>
        /// The size of the floating-point type in bits.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the size is set to a negative value or a value not divisible by 8.
        /// </exception>
        public virtual int Size
        {
            get
            {
                return size;
            }
            set
            {
                if (value < 0 || value % sizeof(byte) != 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Floating-point size must be a nonnegative number of bits.");
                }

                size = value;
            }
        }

        /// <summary>
        /// Creates a new floating-point resolution.
        /// </summary>
        /// <param name="type">
        /// The floating-point type.
        /// </param>
        /// <param name="size">
        /// The size of the floating-point type in bits.
        /// </param>
        /// <param name="isNullable">
        /// Whether the floating-point type can have a null value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the size is less than zero or not divisible by 8.
        /// </exception>
        public FloatingPointResolution(Type type, int size, bool isNullable = false) : base(type, isNullable)
        {
            Size = size;
        }
    }

    /// <summary>
    /// Contains resolved information about an integer type.
    /// </summary>
    public class IntegerResolution : TypeResolution
    {
        private int size;

        /// <summary>
        /// Whether the integer type supports negative values.
        /// </summary>
        public virtual bool IsSigned { get; set; }

        /// <summary>
        /// The size of the integer type in bits.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the size is set to a negative value or a value not divisible by 8.
        /// </exception>
        public virtual int Size
        {
            get
            {
                return size;
            }
            set
            {
                if (value < 0 || value % sizeof(byte) != 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Integer size must be a nonnegative number of bits.");
                }

                size = value;
            }
        }

        /// <summary>
        /// Creates a new integer resolution.
        /// </summary>
        /// <param name="type">
        /// The integer type.
        /// </param>
        /// <param name="isSigned">
        /// Whether the integer type supports negative values.
        /// </param>
        /// <param name="size">
        /// The size of the integer type in bits.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the size is less than zero or not divisible by 8.
        /// </exception>
        public IntegerResolution(Type type, bool isSigned, int size) : base(type)
        {
            IsSigned = isSigned;
            Size = size;
        }
    }

    /// <summary>
    /// Contains resolved information about a map type.
    /// </summary>
    public class MapResolution : TypeResolution
    {
        private ICollection<ConstructorResolution> constructors = null!;

        private Type keyType = null!;

        private Type valueType = null!;

        /// <summary>
        /// The map constructors.
        /// </summary>
        public virtual ICollection<ConstructorResolution> Constructors
        {
            get
            {
                return constructors ?? throw new InvalidOperationException();
            }
            set
            {
                constructors = value ?? throw new ArgumentNullException(nameof(value), "Resolved constructor collection cannot be null.");
            }
        }

        /// <summary>
        /// The map key type.
        /// </summary>
        public virtual Type KeyType
        {
            get
            {
                return keyType ?? throw new InvalidOperationException();
            }
            set
            {
                keyType = value ?? throw new ArgumentNullException(nameof(value), "Resolved key type cannot be null.");
            }
        }

        /// <summary>
        /// The map value type.
        /// </summary>
        public virtual Type ValueType
        {
            get
            {
                return valueType ?? throw new InvalidOperationException();
            }
            set
            {
                valueType = value ?? throw new ArgumentNullException(nameof(value), "Resolved value type cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new map resolution.
        /// </summary>
        /// <param name="type">
        /// The map type.
        /// </param>
        /// <param name="keyType">
        /// The map key type.
        /// </param>
        /// <param name="valueType">
        /// The map value type.
        /// </param>
        /// <param name="constructors">
        /// The map constructors.
        /// </param>
        /// <param name="isNullable">
        /// Whether the map type can have a null value.
        /// </param>
        public MapResolution(Type type, Type keyType, Type valueType, ICollection<ConstructorResolution>? constructors = null, bool isNullable = false) : base(type, isNullable)
        {
            Constructors = constructors ?? new List<ConstructorResolution>();
            KeyType = keyType;
            ValueType = valueType;
        }
    }

    /// <summary>
    /// Contains resolved information about a UTF-8 string type.
    /// </summary>
    public class StringResolution : TypeResolution
    {
        /// <summary>
        /// Creates a new string resolution.
        /// </summary>
        /// <param name="type">
        /// The string type.
        /// </param>
        /// <param name="isNullable">
        /// Whether the string type can have a null value.
        /// </param>
        public StringResolution(Type type, bool isNullable = false) : base(type, isNullable) { }
    }

    /// <summary>
    /// Contains resolved information about a timestamp type.
    /// </summary>
    public class TimestampResolution : TypeResolution
    {
        private decimal precision;

        /// <summary>
        /// The precision of the timestamp type relative to 1 second. For example, millisecond
        /// precision = 0.001.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is set to a value less than zero.
        /// </exception>
        public virtual decimal Precision
        {
            get
            {
                return precision;
            }
            set
            {
                if (value <= 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), "Precision must be a positive factor.");
                }

                precision = value;
            }
        }

        /// <summary>
        /// Creates a new timestamp resolution.
        /// </summary>
        /// <param name="type">
        /// The timestamp type.
        /// </param>
        /// <param name="precision">
        /// The precision of the timestamp type relative to 1 second. For example, millisecond
        /// precision = 0.001.
        /// </param>
        /// <param name="isNullable">
        /// Whether the timestamp type can have a null value.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException">
        /// Thrown when the precision is less than zero.
        /// </exception>
        public TimestampResolution(Type type, decimal precision, bool isNullable = false) : base(type, isNullable)
        {
            Precision = precision;
        }
    }

    /// <summary>
    /// Contains resolved information about a URI type.
    /// </summary>
    public class UriResolution : TypeResolution
    {
        /// <summary>
        /// Creates a new URI resolution.
        /// </summary>
        /// <param name="type">
        /// The URI type.
        /// </param>
        /// <param name="isNullable">
        /// Whether the URI type can have a null value.
        /// </param>
        public UriResolution(Type type, bool isNullable = false) : base(type, isNullable) { }
    }

    /// <summary>
    /// Contains resolved information about a UUID type.
    /// </summary>
    /// <remarks>
    /// See https://stackoverflow.com/a/6953207 for a rundown of UUID variants/versions.
    /// </remarks>
    public class UuidResolution : TypeResolution
    {
        /// <summary>
        /// The RFC 4122 variant.
        /// </summary>
        public virtual int Variant { get; set; }

        /// <summary>
        /// The RFC 4122 sub-variant.
        /// </summary>
        public virtual int? Version { get; set; }

        /// <summary>
        /// Creates a new UUID resolution.
        /// </summary>
        /// <param name="type">
        /// The UUID type.
        /// </param>
        /// <param name="variant">
        /// The RFC 4122 variant.
        /// </param>
        /// <param name="version">
        /// The RFC 4122 sub-variant.
        /// </param>
        /// <param name="isNullable">
        /// Whether the UUID type can have a null value.
        /// </param>
        public UuidResolution(Type type, int variant, int? version = null, bool isNullable = false) : base(type, isNullable)
        {
            Variant = variant;
            Version = version;
        }
    }

    /// <summary>
    /// Contains resolved information about a named type (i.e., a class, struct, interface, or enum).
    /// </summary>
    public abstract class NamedTypeResolution : TypeResolution
    {
        private IdentifierResolution name = null!;

        /// <summary>
        /// The type name.
        /// </summary>
        public virtual IdentifierResolution Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }
            set
            {
                name = value ?? throw new ArgumentNullException(nameof(value), "Name cannot be null.");
            }
        }

        /// <summary>
        /// The type namespace.
        /// </summary>
        public virtual IdentifierResolution? Namespace { get; set; }

        /// <summary>
        /// Creates a new named type resolution.
        /// </summary>
        /// <param name="type">
        /// The named type.
        /// </param>
        /// <param name="name">
        /// The type name.
        /// </param>
        /// <param name="namespace">
        /// The type namespace.
        /// </param>
        /// <param name="isNullable">
        /// Whether the named type can have a null value.
        /// </param>
        public NamedTypeResolution(Type type, IdentifierResolution name, IdentifierResolution? @namespace = null, bool isNullable = false) : base(type, isNullable)
        {
            Name = name;
            Namespace = @namespace;
        }
    }

    /// <summary>
    /// Contains resolved information about an enum type.
    /// </summary>
    public class EnumResolution : NamedTypeResolution
    {
        private ICollection<SymbolResolution> symbols = null!;

        private Type underlyingType = null!;

        /// <summary>
        /// Whether the enum is a bit flag enum.
        /// </summary>
        public virtual bool IsFlagEnum { get; set; }

        /// <summary>
        /// The enum’s underlying integral type.
        /// </summary>
        public virtual Type UnderlyingType
        {
            get
            {
                return underlyingType ?? throw new InvalidOperationException();
            }
            set
            {
                underlyingType = value ?? throw new ArgumentNullException(nameof(value), "Resolved underlying type cannot be null.");
            }
        }

        /// <summary>
        /// The enum symbols.
        /// </summary>
        public virtual ICollection<SymbolResolution> Symbols
        {
            get
            {
                return symbols ?? throw new InvalidOperationException();
            }
            set
            {
                symbols = value ?? throw new ArgumentNullException(nameof(value), "Enum symbol collection cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new enum type resolution.
        /// </summary>
        /// <param name="type">
        /// The enum type.
        /// </param>
        /// <param name="underlyingType">
        /// The enum’s underlying integral type.
        /// </param>
        /// <param name="name">
        /// The enum type name.
        /// </param>
        /// <param name="namespace">
        /// The enum type namespace.
        /// </param>
        /// <param name="isFlagEnum">
        /// Whether the enum is a bit flag enum.
        /// </param>
        /// <param name="symbols">
        /// The enum symbols. If no symbol collection is supplied, <see cref="Symbols" /> will be
        /// empty.
        /// </param>
        /// <param name="isNullable">
        /// Whether the enum type can have a null value.
        /// </param>
        public EnumResolution(Type type, Type underlyingType, IdentifierResolution name, IdentifierResolution? @namespace = null, bool isFlagEnum = false, ICollection<SymbolResolution>? symbols = null, bool isNullable = false) : base(type, name, @namespace, isNullable)
        {
            IsFlagEnum = isFlagEnum;
            Symbols = symbols ?? new List<SymbolResolution>();
            UnderlyingType = underlyingType;
        }
    }

    /// <summary>
    /// Contains resolved information about a record type.
    /// </summary>
    public class RecordResolution : NamedTypeResolution
    {
        private ICollection<ConstructorResolution> constructors = null!;

        private ICollection<FieldResolution> fields = null!;

        /// <summary>
        /// The record constructors.
        /// </summary>
        public virtual ICollection<ConstructorResolution> Constructors
        {
            get
            {
                return constructors ?? throw new InvalidOperationException();
            }
            set
            {
                constructors = value ?? throw new ArgumentNullException(nameof(value), "Record constructor collection cannot be null.");
            }
        }

        /// <summary>
        /// The record fields.
        /// </summary>
        public virtual ICollection<FieldResolution> Fields
        {
            get
            {
                return fields ?? throw new InvalidOperationException();
            }
            set
            {
                fields = value ?? throw new ArgumentNullException(nameof(value), "Record field collection cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new record type resolution.
        /// </summary>
        /// <param name="type">
        /// The record type.
        /// </param>
        /// <param name="name">
        /// The record type name.
        /// </param>
        /// <param name="namespace">
        /// The record type namespace.
        /// </param>
        /// <param name="fields">
        /// The record fields. If no fields collection is supplied, <see cref="Fields" /> will be
        /// empty.
        /// </param>
        /// <param name="constructors">
        /// The constructors for the underlying type.
        /// </param>
        /// <param name="isNullable">
        /// Whether the record type can have a null value.
        /// </param>
        public RecordResolution(Type type, IdentifierResolution name, IdentifierResolution? @namespace = null, ICollection<FieldResolution>? fields = null, ICollection<ConstructorResolution>? constructors = null, bool isNullable = false) : base(type, name, @namespace, isNullable)
        {
            Fields = fields ?? new List<FieldResolution>();
            Constructors = constructors ?? new List<ConstructorResolution>();
        }
    }
}
