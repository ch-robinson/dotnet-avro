namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Represents an Avro schema that defines a set of string constants (symbols). All symbols in
    /// an enum must be unique.
    /// </summary>
    /// <remarks>
    /// See the <a href="https://avro.apache.org/docs/current/spec.html#Enums">Avro spec</a> for
    /// details.
    /// </remarks>
    public class EnumSchema : NamedSchema
    {
        private ConstrainedSet<string> symbols = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="EnumSchema" /> class.
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
        public EnumSchema(string name, IEnumerable<string>? symbols = null)
            : base(name)
        {
            Symbols = symbols?.ToArray() ?? Array.Empty<string>();
        }

        /// <summary>
        /// Gets or sets the human-readable description of the enum.
        /// </summary>
        public string? Documentation { get; set; }

        /// <summary>
        /// Gets or sets the enum symbols.
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
    }
}
