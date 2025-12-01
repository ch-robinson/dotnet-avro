namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
#if !NET6_0_OR_GREATER
    using System.Numerics;
#endif
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Implements a <see cref="BinarySerializerBuilder" /> case that matches <see cref="DecimalLogicalType" />
    /// and attempts to map it to any provided type.
    /// </summary>
    public class BinaryDecimalSerializerBuilderCase : DecimalSerializerBuilderCase, IBinarySerializerBuilderCase
    {
        /// <summary>
        /// Builds a <see cref="BinarySerializer{T}" /> for a <see cref="DecimalLogicalType" />.
        /// </summary>
        /// <returns>
        /// A successful <see cref="BinarySerializerBuilderCaseResult" /> if <paramref name="schema" />
        /// has a <see cref="DecimalLogicalType" />; an unsuccessful <see cref="BinarySerializerBuilderCaseResult" />
        /// otherwise.
        /// </returns>
        /// <exception cref="UnsupportedSchemaException">
        /// Thrown when <paramref name="schema" /> is not a <see cref="BytesSchema" /> or a
        /// <see cref="FixedSchema "/>.
        /// </exception>
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when <paramref name="type" /> cannot be converted to <see cref="decimal" />.
        /// </exception>
        /// <inheritdoc />
        public virtual BinarySerializerBuilderCaseResult BuildExpression(Expression value, Type type, Schema schema, BinarySerializerBuilderContext context)
        {
            if (schema.LogicalType is DecimalLogicalType decimalLogicalType)
            {
                var precision = Expression.Constant(decimalLogicalType.Precision);
                var scale = Expression.Constant(decimalLogicalType.Scale);

                Expression expression;

                try
                {
                    expression = BuildConversion(value, typeof(decimal));
                }
                catch (InvalidOperationException exception)
                {
                    throw new UnsupportedTypeException(type, $"Failed to map {schema} to {type}.", exception);
                }

                // figure out how to write:
                if (schema is BytesSchema)
                {
                    var writeBytes = typeof(DecimalEncoder)
                        .GetMethod(nameof(DecimalEncoder.WriteDecimalBytes), BindingFlags.Static | BindingFlags.Public)!;

                    expression = Expression.Block(
                        expression,
                        Expression.Call(writeBytes, context.Writer, expression, precision, scale));
                }
                else if (schema is FixedSchema fixedSchema)
                {
                    var writeFixed = typeof(DecimalEncoder)
                        .GetMethod(nameof(DecimalEncoder.WriteDecimalFixed), BindingFlags.Static | BindingFlags.Public)!;

                    expression = Expression.Block(
                        expression,
                        Expression.Call(writeFixed, context.Writer, expression, precision, scale, Expression.Constant(fixedSchema)));
                }
                else
                {
                    throw new UnsupportedSchemaException(schema);
                }

                return BinarySerializerBuilderCaseResult.FromExpression(expression);
            }
            else
            {
                return BinarySerializerBuilderCaseResult.FromException(new UnsupportedSchemaException(schema, $"{nameof(BinaryDecimalSerializerBuilderCase)} can only be applied schemas with a {nameof(DecimalLogicalType)}."));
            }
        }

        private static class DecimalEncoder
        {
            public static void WriteDecimalFixed(BinaryWriter writer, decimal value, int precision, int scale, FixedSchema schema)
            {
#if NET6_0_OR_GREATER
                Span<byte> buffer = stackalloc byte[128];
                var length = EncodeDecimalWith256bitMath(value, scale, buffer);
                if (length != schema.Size)
                {
                    throw new OverflowException($"Size mismatch between {schema} (size {schema.Size}) and decimal with precision {precision} and scale {scale}.");
                }

                writer.WriteFixed(buffer.Slice(0, length));
#else
                var bytes = EncodeWithBigInteger(value, scale);
                if (bytes.Length != schema.Size)
                {
                    throw new OverflowException($"Size mismatch between {schema} (size {schema.Size}) and decimal with precision {precision} and scale {scale}.");
                }

                writer.WriteFixed(bytes);
#endif
            }

            public static void WriteDecimalBytes(BinaryWriter writer, decimal value, int precision, int scale)
            {
#if NET6_0_OR_GREATER
                Span<byte> buffer = stackalloc byte[128];
                var length = EncodeDecimalWith256bitMath(value, scale, buffer);
                writer.WriteBytes(buffer.Slice(0, length));
#else
                var bytes = EncodeWithBigInteger(value, scale);
                writer.WriteBytes(bytes);
#endif
            }

#if NET6_0_OR_GREATER
            public static int EncodeDecimalWith256bitMath(decimal value, int schemaScale, Span<byte> destination)
            {
                // 8 uints = 256 bits.
                // Decimal MaxValue is ~7.9e28 (96 bits).
                // Max supportable scale shift is ~10^48, which covers all realistic Avro cases.
                const int BufferLength = 8;

                Span<int> bits = stackalloc int[4];
                decimal.GetBits(value, bits);
                var lo = bits[0];
                var mid = bits[1];
                var hi = bits[2];
                var flags = bits[3];

                var isNegative = (flags & 0x80000000) != 0;
                var sourceScale = (byte)((flags >> 16) & 0x7F);

                // Load Magnitude into 256-bit stack buffer (Little Endian uints)
                // Buffer represents the absolute unscaled integer value.
                Span<uint> buffer = stackalloc uint[BufferLength];
                buffer.Clear(); // Ensure high bits are zero
                buffer[0] = (uint)lo;
                buffer[1] = (uint)mid;
                buffer[2] = (uint)hi;

                // Apply Scale Adjustment
                var scaleDiff = schemaScale - sourceScale;

                if (scaleDiff > 0)
                {
                    // Schema needs more precision: Multiply by 10^scaleDiff
                    // To avoid overflow, we do this iteratively or in chunks.
                    // Since 256 bits is huge, straightforward multiplication is safe for reasonable scales.
                    if (!TryMultiplyByPowerOfTen(buffer, scaleDiff))
                    {
                        throw new OverflowException();
                    }
                }
                else if (scaleDiff < 0)
                {
                    // Schema needs less precision: Divide by 10^|scaleDiff|
                    DivideByPowerOfTen(buffer, Math.Abs(scaleDiff));
                }

                // Handle Sign (Two's Complement Negation)
                if (isNegative)
                {
                    Negate256(buffer);
                }

                // Serialize to Bytes (Big Endian)
                // We write the full 256 bits to a temp stack buffer, then trim.
                Span<byte> fullBytes = stackalloc byte[BufferLength * 4];
                WriteBigEndian(buffer, fullBytes);

                // Trim leading bytes
                // If Positive: Remove leading 0x00, ensuring MSB of first kept byte is 0 (else prepend 0x00)
                // If Negative: Remove leading 0xFF, ensuring MSB of first kept byte is 1 (else prepend 0xFF)
                //
                // Check the sign bit of the raw number (not the decimal sign, but the 256-bit int sign)
                // Since we manually Negated, the buffer is already in Two's Complement.
                // Check the most significant byte.
                int startIndex = 0;
                if (!isNegative)
                {
                    // Skipping leading 0x00
                    while (startIndex < fullBytes.Length && fullBytes[startIndex] == 0)
                    {
                        startIndex++;
                    }

                    if (startIndex == fullBytes.Length)
                    {
                        // Value is 0
                        startIndex--;
                    }
                    else if ((fullBytes[startIndex] & 0x80) != 0)
                    {
                        // If the first byte we keep looks negative (MSB set), we must keep one zero byte
                        startIndex--;
                    }
                }
                else
                {
                    // Skipping leading 0xFF
                    while (startIndex < fullBytes.Length && fullBytes[startIndex] == 0xFF)
                    {
                        startIndex++;
                    }

                    if (startIndex == fullBytes.Length)
                    {
                        // Value is -1
                        startIndex--;
                    }
                    else if ((fullBytes[startIndex] & 0x80) == 0)
                    {
                        // If the first byte we keep looks positive (MSB clear), we must keep one FF byte
                        startIndex--;
                    }
                }

                var contentLen = fullBytes.Length - startIndex;
                fullBytes.Slice(startIndex, contentLen).CopyTo(destination);
                return contentLen;
            }

            // --- 256-bit Math Helpers ---
            private static bool TryMultiplyByPowerOfTen(Span<uint> buffer, int power)
            {
                // Optimization: Multiply by 10 repeatedly.
                // For scale differences < 50, this is extremely fast on stack memory.
                for (int i = 0; i < power; i++)
                {
                    if (!TryMultiplyByInt(buffer, 10))
                    {
                        return false;
                    }
                }

                return true;
            }

            private static void DivideByPowerOfTen(Span<uint> buffer, int power)
            {
                for (int i = 0; i < power; i++)
                {
                    DivideByInt(buffer, 10);
                }
            }

            // Multiplies the 256-bit number by a 32-bit factor. Returns false on overflow.
            private static bool TryMultiplyByInt(Span<uint> buffer, uint factor)
            {
                var carry = 0UL;
                for (int i = 0; i < buffer.Length; i++)
                {
                    var result = ((ulong)buffer[i] * factor) + carry;
                    buffer[i] = (uint)result;
                    carry = result >> 32;
                }

                return carry == 0;
            }

            // Divides the 256-bit number by a 32-bit divisor.
            private static void DivideByInt(Span<uint> buffer, uint divisor)
            {
                var remainder = 0UL;

                // Iterate from High to Low
                for (int i = buffer.Length - 1; i >= 0; i--)
                {
                    var current = (remainder << 32) | buffer[i];
                    buffer[i] = (uint)(current / divisor);
                    remainder = current % divisor;
                }
            }

            // Performs Two's Complement Negation (~x + 1) on the 256-bit buffer.
            private static void Negate256(Span<uint> buffer)
            {
                // The "+ 1" part of Two's Complement
                var carry = 1UL;
                for (var i = 0; i < buffer.Length; i++)
                {
                    // Invert bits
                    buffer[i] = ~buffer[i];
                    var result = buffer[i] + carry;
                    buffer[i] = (uint)result;
                    carry = result >> 32;
                }
            }

            // Writes 256-bit Little Endian uint buffer to Big Endian byte buffer
            private static void WriteBigEndian(Span<uint> source, Span<byte> destination)
            {
                for (var i = 0; i < source.Length; i++)
                {
                    // Source is Little Endian (index 0 is LSB)
                    // Destination is Big Endian (index 0 is MSB)
                    // We want source[7] to go to destination[0..3]
                    var val = source[source.Length - 1 - i];
                    System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(destination.Slice(i * 4), val);
                }
            }
#else
            private static byte[] EncodeWithBigInteger(decimal value, int scale) {
                var fraction = new BigInteger(value) * BigInteger.Pow(10, scale);
                var whole = new BigInteger((value % 1m) * (decimal)Math.Pow(10, scale));
                var bytes = (fraction + whole).ToByteArray();
                Array.Reverse(bytes);
                return bytes;
            }
#endif
        }
    }
}
