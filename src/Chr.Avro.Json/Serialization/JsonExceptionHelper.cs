namespace Chr.Avro.Serialization
{
    using System.Text.Json;

    /// <summary>
    /// Methods to simplify the creation of serialization exceptions.
    /// </summary>
    internal static class JsonExceptionHelper
    {
        /// <summary>
        /// Creates an exception indicating a value with an unexpected size.
        /// </summary>
        /// <param name="reader">
        /// A <see cref="Utf8JsonReader" /> instance at the position of the value.
        /// </param>
        /// <param name="expectedSize">
        /// The expected size of the value.
        /// </param>
        /// <param name="receivedSize">
        /// The actual size of the value.
        /// </param>
        /// <returns>
        /// An <see cref="InvalidEncodingException" /> with details about the unexpected value.
        /// </returns>
        public static InvalidEncodingException GetUnexpectedSizeException(ref Utf8JsonReader reader, int expectedSize, int receivedSize)
        {
            return new InvalidEncodingException(
                reader.TokenStartIndex,
                $"Expected value of size {expectedSize}; received value of size {receivedSize}.");
        }

        /// <summary>
        /// Creates an exception indicating an unexpected JSON token.
        /// </summary>
        /// <param name="reader">
        /// A <see cref="Utf8JsonReader" /> instance at the position of the token.
        /// </param>
        /// <param name="expectedTokenTypes">
        /// A list of token types that would have been considered valid.
        /// </param>
        /// <returns>
        /// An <see cref="InvalidEncodingException" /> with details about the unexpected token.
        /// </returns>
        public static InvalidEncodingException GetUnexpectedTokenException(ref Utf8JsonReader reader, params JsonTokenType[] expectedTokenTypes)
        {
            return new InvalidEncodingException(
                reader.TokenStartIndex,
                $"Expected token of type {string.Join(" or ", expectedTokenTypes)}; received {reader.TokenType}.");
        }

        /// <summary>
        /// Creates an exception indicating an unknown record field name.
        /// </summary>
        /// <param name="reader">
        /// A <see cref="Utf8JsonReader" /> instance at the position of the name.
        /// </param>
        /// <returns>
        /// An <see cref="InvalidEncodingException" /> with details about the unexpected name.
        /// </returns>
        public static InvalidEncodingException GetUnknownRecordFieldException(ref Utf8JsonReader reader)
        {
            return new InvalidEncodingException(
                reader.TokenStartIndex,
                $"Unknown record field {reader.GetString()}.");
        }

        /// <summary>
        /// Creates an exception indicating an unknown union schema member.
        /// </summary>
        /// <param name="reader">
        /// A <see cref="Utf8JsonReader" /> instance at the position of the member name.
        /// </param>
        /// <returns>
        /// An <see cref="InvalidEncodingException" /> with details about the unexpected name.
        /// </returns>
        public static InvalidEncodingException GetUnknownUnionMemberException(ref Utf8JsonReader reader)
        {
            return new InvalidEncodingException(
                reader.TokenStartIndex,
                $"Unknown union member {reader.GetString()}.");
        }
    }
}
