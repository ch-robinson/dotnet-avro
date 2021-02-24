namespace Chr.Avro.Abstract
{
    using System.Text.RegularExpressions;

    /// <summary>
    /// Provides a common base for Avro schemas and related models (such as record fields).
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
        protected static readonly Regex AllowedName = new (@"^[A-Za-z_][A-Za-z0-9_]*$");
    }
}
