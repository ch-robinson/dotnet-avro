namespace Chr.Avro
{
    using System.Text.Encodings.Web;

    /// <summary>
    /// Encoders that can be used to deserialize JSON values according to the Avro spec.
    /// </summary>
    internal static class JsonEncoder
    {
        /// <summary>
        /// A <see cref="JavaScriptEncoder" /> that can be used to write byte arrays encoded as
        /// JSON strings. This encoder is configured to write each character as a Unicode escape
        /// sequence.
        /// </summary>
        public static readonly JavaScriptEncoder Bytes = JavaScriptEncoder.Create();
    }
}
