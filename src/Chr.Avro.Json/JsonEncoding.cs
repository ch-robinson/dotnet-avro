namespace Chr.Avro
{
    using System.Text;

    /// <summary>
    /// Encodings that can be used to deserialize JSON values according to the Avro spec.
    /// </summary>
    internal static class JsonEncoding
    {
        /// <summary>
        /// An <see cref="Encoding" /> that can be used to read byte arrays encoded as JSON
        /// strings. This encoding is configured to throw <see cref="EncoderFallbackException" />
        /// for any out-of-range characters.
        /// </summary>
        public static readonly Encoding Bytes = Encoding.GetEncoding(
            "iso-8859-1", // 0x00 through 0xff (Encoding.Latin1 in .NET 5)
            EncoderFallback.ExceptionFallback,
            DecoderFallback.ExceptionFallback);
    }
}
