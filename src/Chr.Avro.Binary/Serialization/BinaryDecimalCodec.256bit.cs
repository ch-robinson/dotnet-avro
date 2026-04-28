#if NET8_0_OR_GREATER
namespace Chr.Avro.Serialization
{
    using System;
    using System.Buffers.Binary;
    using System.Runtime.InteropServices;
    using System.Runtime.Intrinsics;

    /// <summary>
    /// Methods to encode and decode decimals using 256-bit integer math for intermediate calculations,
    /// avoiding heap allocations that occur with <see cref="System.Numerics.BigInteger"/>.
    /// This supports encoding/decoding of decimals with magnitudes up to 2^256,
    /// which is more than sufficient for decimal's 96-bit mantissa and maximum scale of 28.
    /// </summary>
    internal static partial class BinaryDecimalCodec
    {
        private static ReadOnlySpan<ulong> PowersOfTen => new ulong[]
        {
            1UL,
            10UL,
            100UL,
            1_000UL,
            10_000UL,
            100_000UL,
            1_000_000UL,
            10_000_000UL,
            100_000_000UL,
            1_000_000_000UL,
            10_000_000_000UL,
            100_000_000_000UL,
            1_000_000_000_000UL,
            10_000_000_000_000UL,
            100_000_000_000_000UL,
            1_000_000_000_000_000UL,
            10_000_000_000_000_000UL,
            100_000_000_000_000_000UL,
            1_000_000_000_000_000_000UL,
            10_000_000_000_000_000_000UL, // 10^19
        };

        private static decimal DecodeDecimalWith256BitMath(ReadOnlySpan<byte> bytes, int scale)
        {
            const int BufferLength = 4;

            // stackalloc is zero-initialized; no Clear() needed.
            Span<ulong> buffer = stackalloc ulong[BufferLength];

            bool isNegative = (bytes[0] & 0x80) != 0;

            // Parse big-endian two's complement bytes into a little-endian 256-bit buffer.
            // Copy then reverse is faster than the equivalent byte-by-byte indexed loop.
            Span<byte> bufferBytes = MemoryMarshal.AsBytes(buffer);
            int copyLen = Math.Min(bytes.Length, bufferBytes.Length);
            bytes.Slice(0, copyLen).CopyTo(bufferBytes);
            bufferBytes.Slice(0, copyLen).Reverse();

            // Sign-extend when the input is shorter than 32 bytes.
            if (isNegative && bytes.Length < bufferBytes.Length)
            {
                bufferBytes.Slice(bytes.Length).Fill(0xFF);
            }

            if (isNegative)
            {
                Negate256(buffer);
            }

            // Reduce scale to fit within decimal's maximum of 28.
            if (scale > 28)
            {
                DivideByPowerOfTen(buffer, scale - 28);
                scale = 28;
            }

            // Fast path: if the magnitude fits in 96 bits (decimal's mantissa size),
            // construct the decimal directly without any division.
            if (buffer[2] == 0 && buffer[3] == 0 && (buffer[1] >> 32) == 0)
            {
                return new decimal(
                    (int)(uint)buffer[0],
                    (int)(uint)(buffer[0] >> 32),
                    (int)(uint)buffer[1],
                    isNegative,
                    (byte)scale);
            }

            // Slow path: the magnitude exceeds 96 bits (e.g. decimal.MaxValue encoded with
            // a non-zero scale). Split into integer and fractional parts via DivRem.
            var (fracLo, fracMid, fracHi) = DivRemByPowerOfTen(buffer, scale);

            if (buffer[2] != 0 || buffer[3] != 0 || (buffer[1] >> 32) != 0)
            {
                throw new OverflowException("Decimal value is too large to fit in System.Decimal.");
            }

            var integerPart = new decimal(
                (int)(uint)buffer[0],
                (int)(uint)(buffer[0] >> 32),
                (int)(uint)buffer[1],
                isNegative,
                0);

            var fractionalPart = new decimal(
                (int)fracLo,
                (int)fracMid,
                (int)fracHi,
                isNegative,
                (byte)scale);

            return integerPart + fractionalPart;
        }

        private static int EncodeDecimalWith256BitMath(decimal value, int schemaScale, Span<byte> destination)
        {
            // 4 ulongs = 256 bits.
            const int BufferLength = 4;

            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);

            var isNegative = (bits[3] & 0x80000000) != 0;
            var sourceScale = (byte)((bits[3] >> 16) & 0x7F);

            // Load magnitude into a 256-bit stack buffer (little-endian ulongs).
            // Decimal magnitude is 96 bits; pack into two ulongs.
            // stackalloc is zero-initialized; buffer[2] and buffer[3] start at 0.
            Span<ulong> buffer = stackalloc ulong[BufferLength];
            buffer[0] = (uint)bits[0] | ((ulong)(uint)bits[1] << 32);
            buffer[1] = (uint)bits[2];

            var scaleDiff = schemaScale - sourceScale;
            if (scaleDiff > 0)
            {
                if (!TryMultiplyByPowerOfTen(buffer, scaleDiff))
                {
                    throw new OverflowException();
                }
            }
            else if (scaleDiff < 0)
            {
                DivideByPowerOfTen(buffer, Math.Abs(scaleDiff));
            }

            if (isNegative)
            {
                Negate256(buffer);
            }

            // Serialize to big-endian bytes.
            Span<byte> fullBytes = stackalloc byte[BufferLength * 8];
            WriteBigEndian(buffer, fullBytes);

            // Trim leading padding bytes based on two's complement sign.
            int startIndex = 0;
            if (!isNegative)
            {
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
                    // Must prepend 0x00 so it isn't interpreted as negative
                    startIndex--;
                }
            }
            else
            {
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
                    // Must prepend 0xFF so it remains negative
                    startIndex--;
                }
            }

            var contentLen = fullBytes.Length - startIndex;
            fullBytes.Slice(startIndex, contentLen).CopyTo(destination);
            return contentLen;
        }

        private static bool TryMultiplyByPowerOfTen(Span<ulong> buffer, int power)
        {
            while (power >= 19)
            {
                if (!TryMultiplyByInt(buffer, PowersOfTen[19]))
                {
                    return false;
                }

                power -= 19;
            }

            return power == 0 || TryMultiplyByInt(buffer, PowersOfTen[power]);
        }

        private static bool TryMultiplyByInt(Span<ulong> buffer, ulong factor)
        {
            // Find the highest non-zero word to avoid pointless multiplications.
            int len = buffer.Length;
            while (len > 1 && buffer[len - 1] == 0)
            {
                len--;
            }

            ulong carry = 0;
            for (int i = 0; i < len; i++)
            {
                UInt128 product = ((UInt128)buffer[i] * factor) + carry;
                buffer[i] = (ulong)product;
                carry = (ulong)(product >> 64);
            }

            if (carry == 0)
            {
                return true;
            }

            if (len >= buffer.Length)
            {
                // overflow
                return false;
            }

            buffer[len] = carry;
            return true;
        }

        private static void WriteBigEndian(Span<ulong> source, Span<byte> destination)
        {
            BinaryPrimitives.WriteUInt64BigEndian(destination, source[3]);
            BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(8), source[2]);
            BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(16), source[1]);
            BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(24), source[0]);
        }

        // Divides buffer by 10^scale in-place and returns the fractional component as (Lo, Mid, Hi).
        // The remainder satisfies: rem < 10^scale ≤ 10^28 < 2^93, so it always fits in 96 bits.
        private static (uint Lo, uint Mid, uint Hi) DivRemByPowerOfTen(Span<ulong> buffer, int scale)
        {
            if (scale == 0)
            {
                return (0, 0, 0);
            }

            // For scale ≤ 19, a single 64-bit division suffices.
            int step1 = Math.Min(scale, 19);
            ulong rem1 = DivRemByInt(buffer, PowersOfTen[step1]);

            int step2 = scale - step1;
            if (step2 == 0)
            {
                return ((uint)rem1, (uint)(rem1 >> 32), 0);
            }

            // For scale > 19, divide by 10^(scale-19) in a second step.
            // Full remainder = rem2 * 10^step1 + rem1, fits in 96 bits since scale ≤ 28.
            ulong rem2 = DivRemByInt(buffer, PowersOfTen[step2]);
            UInt128 fullRemainder = (UInt128)(rem2 * PowersOfTen[step1]) + rem1;
            return ((uint)fullRemainder, (uint)(fullRemainder >> 32), (uint)(fullRemainder >> 64));
        }

        // Divides buffer by divisor in-place, returning the remainder.
        // Only processes words up to the highest non-zero one, skipping useless UInt128 divisions.
        private static ulong DivRemByInt(Span<ulong> buffer, ulong divisor)
        {
            // Skip leading zero words (buffer is little-endian, so scan from the top down).
            int start = buffer.Length - 1;
            while (start > 0 && buffer[start] == 0)
            {
                start--;
            }

            ulong remainder = 0;
            for (int i = start; i >= 0; i--)
            {
                UInt128 current = ((UInt128)remainder << 64) | buffer[i];
                buffer[i] = (ulong)(current / divisor);
                remainder = (ulong)(current % divisor);
            }

            return remainder;
        }

        private static void DivideByPowerOfTen(Span<ulong> buffer, int power)
        {
            while (power >= 19)
            {
                DivRemByInt(buffer, PowersOfTen[19]);
                power -= 19;
            }

            if (power > 0)
            {
                DivRemByInt(buffer, PowersOfTen[power]);
            }
        }

        private static void Negate256(Span<ulong> buffer)
        {
            ref byte bufferBytes = ref MemoryMarshal.GetReference(MemoryMarshal.AsBytes(buffer));
            var vec = Vector256.LoadUnsafe(ref bufferBytes);
            (~vec).StoreUnsafe(ref bufferBytes);

            // Add 1 for Two's Complement
            for (int i = 0; i < buffer.Length; i++)
            {
                if (++buffer[i] != 0)
                {
                    break;
                }
            }
        }
    }
}
#endif
