namespace Chr.Avro.Resolution
{
    using System;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Represents resolved information about a name.
    /// </summary>
    public class IdentifierResolution
    {
        /// <summary>
        /// A regular expression that matches non-alphanumeric Unicode characters. These character
        /// classes are derived from the C# identifier spec (ECMA-334 5th Edition, section 7.4.3).
        /// </summary>
        private static readonly Regex FuzzyCharacters = new (@"[^\p{L}\p{Nd}\p{Nl}]");

        private string value = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="IdentifierResolution" /> class.
        /// </summary>
        /// <param name="value">
        /// The resolved name.
        /// </param>
        /// <param name="isSetExplicitly">
        /// Whether the name was set explicitly (e.g., by an annotation).
        /// </param>
        public IdentifierResolution(string value, bool isSetExplicitly = false)
        {
            Value = value;
            IsSetExplicitly = isSetExplicitly;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the name was set explicitly (e.g., by an
        /// annotation).
        /// </summary>
        public virtual bool IsSetExplicitly { get; set; }

        /// <summary>
        /// Gets or sets the resolved name.
        /// </summary>
        public virtual string Value
        {
            get
            {
                return value ?? throw new InvalidOperationException();
            }

            set
            {
                this.value = value ?? throw new ArgumentNullException(nameof(value), "Resolved name cannot be null.");
            }
        }

        /// <summary>
        /// Determines whether the resolved name matches another resolved name.
        /// </summary>
        /// <param name="other">
        /// The resolved name to compare.
        /// </param>
        /// <returns>
        /// A value indicating whether <paramref name="other" /> matches <see cref="Value" />.
        /// </returns>
        public virtual bool IsMatch(IdentifierResolution other)
        {
            return IsMatch(other.Value);
        }

        /// <summary>
        /// Determines whether the resolved name matches another name.
        /// </summary>
        /// <param name="other">
        /// The name to compare.
        /// </param>
        /// <returns>
        /// A value indicating whether <paramref name="other" /> matches <see cref="Value" />.
        /// </returns>
        public virtual bool IsMatch(string other)
        {
            return IsSetExplicitly
                ? Value == other
                : Normalize(Value).Equals(Normalize(other), StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary>
        /// Normalizes a string for fuzzy comparison.
        /// </summary>
        /// <param name="value">
        /// The string to prepare for comparison.
        /// </param>
        /// <returns>
        /// <paramref name="value" /> with all non-alphanumeric characters removed.
        /// </returns>
        protected virtual string Normalize(string value)
        {
            return FuzzyCharacters.Replace(value, string.Empty);
        }
    }
}
