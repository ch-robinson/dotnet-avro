namespace Chr.Avro.Serialization
{
    using System;
    using System.Numerics;

    /// <summary>
    /// Methods to encode and decode decimals using <see cref="BigInteger"/> for intermediate calculations.
    /// </summary>
    internal static partial class BinaryDecimalCodec
    {
        private static byte[] EncodeWithBigInteger(decimal value, int scale)
        {
            var fraction = new BigInteger(value) * BigInteger.Pow(10, scale);
            var whole = new BigInteger((value % 1m) * (decimal)Math.Pow(10, scale));
            var bytes = (fraction + whole).ToByteArray();
            Array.Reverse(bytes);
            return bytes;
        }

        private static decimal DecodeDecimalWithBigInteger(byte[] bytes, int scale)
        {
            Array.Reverse(bytes);
            var bigInteger = new BigInteger(bytes);
            var divisor = BigInteger.Pow(10, scale);
            var whole = BigInteger.DivRem(bigInteger, divisor, out var remainder);
            return (decimal)whole + (decimal)remainder / (decimal)Math.Pow(10, scale);
        }
    }
}
