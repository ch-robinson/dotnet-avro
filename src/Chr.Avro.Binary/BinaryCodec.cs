using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

namespace Chr.Avro.Serialization
{
    /// <summary>
    /// Handles reading and writing serialized Avro data.
    /// </summary>
    public interface IBinaryCodec
    {
        /// <summary>
        /// Generates an expression that reads a number of bytes from a stream.
        /// </summary>
        Expression Read(Expression stream, Expression length);

        /// <summary>
        /// Generates an expression that reads blocks from a stream.
        /// </summary>
        Expression ReadBlocks(Expression stream, Expression body);

        /// <summary>
        /// Generates an expression that reads a boolean.
        /// </summary>
        Expression ReadBoolean(Expression stream);

        /// <summary>
        /// Generates an expression that reads key-value blocks from a stream.
        /// </summary>
        Expression ReadDictionary(Expression stream, Expression key, Expression value);

        /// <summary>
        /// Generates an expression that reads a double-precision floating-point number.
        /// </summary>
        Expression ReadDouble(Expression stream);

        /// <summary>
        /// Reads a zig-zag encoded integer.
        /// </summary>
        Expression ReadInteger(Expression stream);

        /// <summary>
        /// Generates an expression that reads item blocks from a stream.
        /// </summary>
        Expression ReadList(Expression stream, Expression item);

        /// <summary>
        /// Reads a single-precision floating-point number.
        /// </summary>
        Expression ReadSingle(Expression stream);

        /// <summary>
        /// Writes bytes to a stream.
        /// </summary>
        void Write(byte[] bytes, Stream stream);

        /// <summary>
        /// Writes blocks to a stream.
        /// </summary>
        void WriteBlocks<T>(IEnumerable<T> items, Action<T, Stream> @delegate, Stream stream);

        /// <summary>
        /// Writes key-value blocks to a stream.
        /// </summary>
        void WriteBlocks<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> items, Action<TKey, Stream> keyDelegate, Action<TValue, Stream> valueDelegate, Stream stream);

        /// <summary>
        /// Writes a boolean.
        /// </summary>
        void WriteBoolean(bool value, Stream stream);

        /// <summary>
        /// Writes a double-precision floating-point number.
        /// </summary>
        void WriteDouble(double value, Stream stream);

        /// <summary>
        /// Writes a zig-zag encoded integer.
        /// </summary>
        void WriteInteger(long value, Stream stream);

        /// <summary>
        /// Writes a single-precision floating-point number.
        /// </summary>
        void WriteSingle(float value, Stream stream);
    }

    /// <summary>
    /// A binary codec implementation.
    /// </summary>
    public class BinaryCodec : IBinaryCodec
    {
        /// <summary>
        /// Generates an expression that reads a number of bytes from a stream.
        /// </summary>
        public virtual Expression Read(Expression stream, Expression count)
        {
            var buffer = Expression.Variable(typeof(byte[]));
            var size = Expression.Variable(typeof(int));

            var read = typeof(Stream)
                .GetMethod(nameof(Stream.Read), new[] { buffer.Type, typeof(int), typeof(int) });

            return Expression.Block(
                new[] { buffer, size },
                Expression.Assign(size, count),
                Expression.Assign(buffer, Expression.NewArrayBounds(buffer.Type.GetElementType(), size)),
                Expression.Call(stream, read, buffer, Expression.Constant(0), size),
                buffer
            );
        }

        /// <summary>
        /// Generates an expression that reads blocks from a stream.
        /// </summary>
        public virtual Expression ReadBlocks(Expression stream, Expression body)
        {
            var index = Expression.Variable(typeof(long));
            var size = Expression.Variable(typeof(long));

            var outer = Expression.Label();
            var inner = Expression.Label();

            return Expression.Block(
                new[] { index, size },
                Expression.Loop(
                    Expression.Block(
                        Expression.Assign(size, ReadInteger(stream)),
                        Expression.IfThen(
                            Expression.Equal(size, Expression.Constant(0L)),
                            Expression.Break(outer)),

                        // negative size indicates that the number of bytes in the block follows,
                        // so discard:
                        Expression.IfThen(
                            Expression.LessThan(size, Expression.Constant(0L)),
                            Expression.Block(
                                Expression.MultiplyAssign(size, Expression.Constant(-1L)),
                                ReadInteger(stream))),

                        Expression.Assign(index, Expression.Constant(0L)),
                        Expression.Loop(
                            Expression.Block(
                                Expression.IfThen(
                                    Expression.Equal(Expression.PostIncrementAssign(index), size),
                                    Expression.Break(inner)),
                                body),
                            inner)),
                    outer));
        }

        /// <summary>
        /// Generates an expression that reads a boolean.
        /// </summary>
        /// <remarks>
        /// Unlike some implementations, Chr.Avro treats any non-zero byte as true.
        /// </remarks>
        public virtual Expression ReadBoolean(Expression stream)
        {
            var readByte = typeof(Stream)
                .GetMethod(nameof(Stream.ReadByte), Type.EmptyTypes);

            return Expression.NotEqual(Expression.Call(stream, readByte), Expression.Constant(0x00));
        }

        /// <summary>
        /// Generates an expression that reads key-value blocks from a stream.
        /// </summary>
        public virtual Expression ReadDictionary(Expression stream, Expression key, Expression value)
        {
            var constructor = typeof(Dictionary<,>)
                .MakeGenericType(key.Type, value.Type)
                .GetConstructor(Type.EmptyTypes);

            var dictionary = Expression.Variable(constructor.DeclaringType);

            var add = typeof(IDictionary<,>)
                .MakeGenericType(key.Type, value.Type)
                .GetMethod("Add", new[] { key.Type, value.Type });

            return Expression.Block(
                new[] { dictionary },
                Expression.Assign(dictionary, Expression.New(constructor)),
                ReadBlocks(
                    stream,
                    Expression.Call(dictionary, add, key, value)),
                dictionary);
        }

        /// <summary>
        /// Generates an expression that reads a double-precision floating-point number.
        /// </summary>
        public virtual Expression ReadDouble(Expression stream)
        {
            var expression = Read(stream, Expression.Constant(8));

            if (!BitConverter.IsLittleEndian)
            {
                var buffer = Expression.Variable(expression.Type);
                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { expression.Type });

                expression = Expression.Block(
                    new[] { buffer },
                    Expression.Assign(buffer, expression),
                    Expression.Call(null, reverse, buffer),
                    buffer);
            }

            var toDouble = typeof(BitConverter)
                .GetMethod(nameof(BitConverter.ToDouble), new[] { expression.Type, typeof(int) });

            return Expression.Call(null, toDouble, expression, Expression.Constant(0));
        }

        /// <summary>
        /// Reads a zig-zag encoded integer.
        /// </summary>
        /// <exception cref="OverflowException">
        /// Thrown when an encoded integer cannot fit in a <see cref="long" />.
        /// </exception>
        public virtual Expression ReadInteger(Expression stream)
        {
            var read = Expression.Variable(typeof(int));
            var shift = Expression.Variable(typeof(int));
            var result = Expression.Variable(typeof(ulong));
            var chunk = Expression.Variable(typeof(ulong));
            var coerced = Expression.Variable(typeof(long));

            var target = Expression.Label();

            var readByte = typeof(Stream)
                .GetMethod(nameof(Stream.ReadByte), Type.EmptyTypes);

            var exceptionConstructor = typeof(OverflowException)
                .GetConstructor(new[] { typeof(string) });

            return Expression.Block(
                new[] { read, shift, result, chunk, coerced },
                Expression.Assign(read, Expression.Constant(0)),
                Expression.Assign(shift, Expression.Constant(0)),
                Expression.Assign(result, Expression.Constant(0UL)),
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(
                            Expression.Equal(Expression.PostIncrementAssign(read), Expression.Constant(10)),
                            Expression.Throw(Expression.New(exceptionConstructor, Expression.Constant("Encoded integer exceeds long bounds.")))),
                        Expression.Assign(chunk, Expression.Convert(Expression.Call(stream, readByte), chunk.Type)),
                        Expression.OrAssign(result, Expression.LeftShift(Expression.And(chunk, Expression.Constant(0x7FUL)), shift)),
                        Expression.AddAssign(shift, Expression.Constant(7)),
                        Expression.IfThen(
                            Expression.Equal(Expression.And(chunk, Expression.Constant(0x80UL)), Expression.Constant(0UL)),
                            Expression.Break(target))),
                    target),
                Expression.Assign(coerced, Expression.Convert(result, typeof(long))),
                Expression.ExclusiveOr(Expression.Negate(Expression.And(coerced, Expression.Constant(0x1L))), Expression.And(Expression.RightShift(coerced, Expression.Constant(1)), Expression.Constant(0x7FFFFFFFFFFFFFFFL))));
        }

        /// <summary>
        /// Generates an expression that reads item blocks from a stream.
        /// </summary>
        public virtual Expression ReadList(Expression stream, Expression item)
        {
            var constructor = typeof(List<>)
                .MakeGenericType(item.Type)
                .GetConstructor(Type.EmptyTypes);

            var list = Expression.Variable(constructor.DeclaringType);

            var add = typeof(ICollection<>)
                .MakeGenericType(item.Type)
                .GetMethod("Add", new[] { item.Type });

            return Expression.Block(
                new[] { list },
                Expression.Assign(list, Expression.New(constructor)),
                ReadBlocks(
                    stream,
                    Expression.Call(list, add, item)),
                list);
        }

        /// <summary>
        /// Generates an expression that reads a single-precision floating-point number.
        /// </summary>
        public virtual Expression ReadSingle(Expression stream)
        {
            var expression = Read(stream, Expression.Constant(4));

            if (!BitConverter.IsLittleEndian)
            {
                var buffer = Expression.Variable(expression.Type);

                var reverse = typeof(Array)
                    .GetMethod(nameof(Array.Reverse), new[] { expression.Type });

                expression = Expression.Block(
                    new[] { buffer },
                    Expression.Assign(buffer, expression),
                    Expression.Call(null, reverse, buffer),
                    buffer
                );
            }

            var toDouble = typeof(BitConverter)
                .GetMethod(nameof(BitConverter.ToSingle), new[] { expression.Type, typeof(int) });

            return Expression.Call(null, toDouble, expression, Expression.Constant(0));
        }

        /// <summary>
        /// Writes a boolean.
        /// </summary>
        public virtual void Write(byte[] bytes, Stream stream)
        {
            stream.Write(bytes, 0, bytes.Length);
        }

        /// <summary>
        /// Writes blocks to a stream.
        /// </summary>
        public void WriteBlocks<T>(IEnumerable<T> items, Action<T, Stream> @delegate, Stream stream)
        {
            var list = items.ToList();

            if (list.Count > 0)
            {
                WriteInteger(list.LongCount(), stream);

                foreach (var item in list)
                {
                    @delegate(item, stream);
                }
            }

            WriteInteger(0L, stream);
        }

        /// <summary>
        /// Writes key-value blocks to a stream.
        /// </summary>
        public void WriteBlocks<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> items, Action<TKey, Stream> keyDelegate, Action<TValue, Stream> valueDelegate, Stream stream)
        {
            WriteBlocks(items, (p, s) => { keyDelegate(p.Key, s); valueDelegate(p.Value, s); }, stream);
        }

        /// <summary>
        /// Writes a boolean.
        /// </summary>
        public virtual void WriteBoolean(bool value, Stream stream)
        {
            stream.WriteByte((byte)(value ? 1 : 0));
        }

        /// <summary>
        /// Writes a double-precision floating-point number.
        /// </summary>
        public virtual void WriteDouble(double value, Stream stream)
        {
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            Write(bytes, stream);
        }

        /// <summary>
        /// Writes a zig-zag encoded integer.
        /// </summary>
        public virtual void WriteInteger(long value, Stream stream)
        {
            var encoded = unchecked((ulong)((value << 1) ^ (value >> 63)));

            do
            {
                var chunk = encoded & 0x7f;
                encoded >>= 7;

                if (encoded != 0)
                {
                    chunk |= 0x80;
                }

                stream.WriteByte((byte)chunk);
            }
            while (encoded != 0);
        }

        /// <summary>
        /// Writes a single-precision floating-point number.
        /// </summary>
        public virtual void WriteSingle(float value, Stream stream)
        {
            var bytes = BitConverter.GetBytes(value);

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(bytes);
            }

            Write(bytes, stream);
        }
    }
}
