using System;
using System.Text.RegularExpressions;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// Contains resolved information about a name.
    /// </summary>
    public class IdentifierResolution
    {
        private static readonly Regex fuzzyCharacters = new Regex(@"[^A-Za-z0-9]");

        private string value = null!;

        /// <summary>
        /// Whether the name was set explicitly (e.g., in an annotation).
        /// </summary>
        public virtual bool IsSetExplicitly { get; set; }

        /// <summary>
        /// The resolved name.
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
        /// Creates a new identifier resolution.
        /// </summary>
        /// <param name="value">
        /// The resolved name.
        /// </param>
        /// <param name="isSetExplicitly">
        /// Whether the name was set explicitly (e.g., in an annotation).
        /// </param>
        public IdentifierResolution(string value, bool isSetExplicitly = false)
        {
            IsSetExplicitly = isSetExplicitly;
            Value = value;
        }

        /// <summary>
        /// Whether the resolved name matches another resolved name.
        /// </summary>
        /// <param name="other">
        /// The resolved name to compare.
        /// </param>
        public virtual bool IsMatch(IdentifierResolution other)
        {
            return IsMatch(other.Value);
        }

        /// <summary>
        /// Whether the resolved name matches another name.
        /// </summary>
        /// <param name="other">
        /// The name to compare.
        /// </param>
        public virtual bool IsMatch(string other)
        {
            return IsSetExplicitly
                ? Value == other
                : Normalize(Value) == Normalize(other);
        }

        /// <summary>
        /// Normalizes a string for fuzzy comparison.
        /// </summary>
        protected virtual string Normalize(string value)
        {
            return fuzzyCharacters.Replace(value, string.Empty).ToUpperInvariant();
        }
    }
}
