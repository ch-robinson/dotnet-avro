namespace Chr.Avro.Serialization
{
    using System;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides static encode and decode methods for Avro decimal binary representation,
    /// shared by <see cref="BinaryDecimalSerializerBuilderCase" /> and
    /// <see cref="BinaryDecimalDeserializerBuilderCase" />.
    /// </summary>
    internal static partial class BinaryDecimalCodec
    {
        /// <summary>
        /// Writes a <see cref="decimal"/> value to the specified <see cref="BinaryWriter"/> as a fixed-size byte array.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to which the fixed-length decimal bytes will be written.</param>
        /// <param name="value">The <see cref="decimal"/> value to encode and write.</param>
        /// <param name="precision">The total number of digits (precision) of the decimal value.</param>
        /// <param name="scale">The number of fractional digits (scale) of the decimal value.</param>
        /// <param name="schema">The <see cref="FixedSchema"/> defining the expected size of the encoded value.</param>
        /// <exception cref="OverflowException">
        /// Thrown when the size of the encoded byte array does not match the <see cref="FixedSchema.Size"/> defined by the schema.
        /// </exception>
        public static void WriteDecimalFixed(BinaryWriter writer, decimal value, int precision, int scale, FixedSchema schema)
        {
#if NET8_0_OR_GREATER
            // A decimal's 96-bit mantissa fits in at most 13 bytes (12 + sign byte).
            Span<byte> buffer = stackalloc byte[32];
            var length = EncodeDecimalWith256BitMath(value, scale, buffer);
            if (length != schema.Size)
            {
                ThrowOverflow(precision, scale, schema);
                return;
            }

            writer.WriteFixed(buffer.Slice(0, length));
#else
            var bytes = EncodeWithBigInteger(value, scale);
            if (bytes.Length != schema.Size)
            {
                ThrowOverflow(precision, scale, schema);
            }

            writer.WriteFixed(bytes);
#endif
        }

        /// <summary>
        /// Writes a <see cref="decimal"/> value to the specified <see cref="BinaryWriter"/> as a variable-length byte array.
        /// </summary>
        /// <param name="writer">The <see cref="BinaryWriter"/> to which the encoded decimal bytes will be written.</param>
        /// <param name="value">The <see cref="decimal"/> value to encode and write.</param>
        /// <param name="precision">The total number of digits (precision) of the decimal value.</param>
        /// <param name="scale">The number of fractional digits (scale) of the decimal value.</param>
        public static void WriteDecimalBytes(BinaryWriter writer, decimal value, int precision, int scale)
        {
#if NET8_0_OR_GREATER
            // A decimal's 96-bit mantissa fits in at most 13 bytes (12 + sign byte).
            Span<byte> buffer = stackalloc byte[32];
            var length = EncodeDecimalWith256BitMath(value, scale, buffer);
            writer.WriteBytes(buffer.Slice(0, length));
#else
            var bytes = EncodeWithBigInteger(value, scale);
            writer.WriteBytes(bytes);
#endif
        }

        /// <summary>
        /// Decodes a byte array back into a <see cref="decimal"/> value using the specified scale.
        /// </summary>
        /// <param name="bytes">The byte array containing the encoded decimal data.</param>
        /// <param name="scale">The number of fractional digits (scale) to apply to the decoded value.</param>
        /// <returns>The decoded <see cref="decimal"/> value.</returns>
        public static decimal DecodeDecimal(ReadOnlySpan<byte> bytes, int scale)
        {
#if NET8_0_OR_GREATER
            return DecodeDecimalWith256BitMath(bytes, scale);
#else
            return DecodeDecimalWithBigInteger(bytes.ToArray(), scale);
#endif
        }

#if NET8_0_OR_GREATER
        [System.Diagnostics.CodeAnalysis.DoesNotReturn]
#endif
        private static void ThrowOverflow(int precision, int scale, FixedSchema schema)
        {
            throw new OverflowException($"Size mismatch between {schema} (size {schema.Size}) and decimal with precision {precision} and scale {scale}.");
        }
    }
}
