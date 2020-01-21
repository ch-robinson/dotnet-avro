using System;
using System.Collections;
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
        /// Generates an expression that reads item blocks from a stream.
        /// </summary>
        Expression ReadArray(Expression stream, Expression createCollection, Expression readItem);

        /// <summary>
        /// Generates an expression that reads a boolean.
        /// </summary>
        Expression ReadBoolean(Expression stream);

        /// <summary>
        /// Generates an expression that reads a double-precision floating-point number.
        /// </summary>
        Expression ReadDouble(Expression stream);

        /// <summary>
        /// Generates an expression that reads a zig-zag encoded integer.
        /// </summary>
        Expression ReadInteger(Expression stream);

        /// <summary>
        /// Generates an expression that reads key-value blocks from a stream.
        /// </summary>
        Expression ReadMap(Expression stream, Expression createDictionary, Expression readKey, Expression readValue);

        /// <summary>
        /// Generates an expression that reads a single-precision floating-point number.
        /// </summary>
        Expression ReadSingle(Expression stream);

        /// <summary>
        /// Generates an expression that writes bytes to a stream.
        /// </summary>
        Expression Write(Expression bytes, Expression stream);

        /// <summary>
        /// Generates an expression that writes item blocks to a stream.
        /// </summary>
        Expression WriteArray(Expression items, ParameterExpression item, Expression writeItem, Expression stream);

        /// <summary>
        /// Generates an expression that writes a boolean.
        /// </summary>
        Expression WriteBoolean(Expression value, Expression stream);

        /// <summary>
        /// Generates an expression that writes a single- or double-precision floating-point number.
        /// </summary>
        Expression WriteFloat(Expression value, Expression stream);

        /// <summary>
        /// Generates an expression that writes a zig-zag encoded integer.
        /// </summary>
        Expression WriteInteger(Expression value, Expression stream);

        /// <summary>
        /// Generates an expression that writes key-value blocks to a stream.
        /// </summary>
        Expression WriteMap(Expression pairs, ParameterExpression key, ParameterExpression value, Expression writeKey, Expression writeValue, Expression stream);
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
        /// Generates an expression that reads item blocks from a stream.
        /// </summary>
        public virtual Expression ReadArray(Expression stream, Expression createCollection, Expression readItem)
        {
            var collection = Expression.Variable(createCollection.Type);
            var add = collection.Type.GetMethod("Add", new[] { readItem.Type });

            return Expression.Block(
                new[] { collection },
                Expression.Assign(collection, createCollection),
                ReadBlocks(
                    stream,
                    Expression.Call(collection, add, readItem)),
                collection);
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
        /// Generates an expression that reads key-value blocks from a stream.
        /// </summary>
        public virtual Expression ReadMap(Expression stream, Expression createDictionary, Expression readKey, Expression readValue)
        {
            var dictionary = Expression.Variable(createDictionary.Type);
            var add = dictionary.Type.GetMethod("Add", new[] { readKey.Type, readValue.Type });

            return Expression.Block(
                new[] { dictionary },
                Expression.Assign(dictionary, createDictionary),
                ReadBlocks(
                    stream,
                    Expression.Call(dictionary, add, readKey, readValue)),
                dictionary);
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
        /// Generates an expression that writes bytes to a stream.
        /// </summary>
        public virtual Expression Write(Expression bytes, Expression stream)
        {
            var write = typeof(Stream)
                .GetMethod(nameof(Stream.Write), new[] { bytes.Type, typeof(int), typeof(int) });

            return Expression.Call(stream, write, bytes, Expression.Constant(0), Expression.ArrayLength(bytes));
        }

        /// <summary>
        /// Generates an expression that writes item blocks to a stream.
        /// </summary>
        public Expression WriteArray(Expression items, ParameterExpression item, Expression writeItem, Expression stream)
        {
            var collection = Expression.Variable(typeof(ICollection<>).MakeGenericType(item.Type));
            var enumerable = Expression.Variable(typeof(IEnumerable<>).MakeGenericType(item.Type));
            var enumerator = Expression.Variable(typeof(IEnumerator<>).MakeGenericType(item.Type));

            var loop = Expression.Label();

            var dispose = typeof(IDisposable)
                .GetMethod(nameof(IDisposable.Dispose), Type.EmptyTypes);

            var getCount = collection.Type
                .GetProperty("Count")
                .GetGetMethod();

            var getCurrent = enumerator.Type
                .GetProperty(nameof(IEnumerator.Current))
                .GetGetMethod();

            var getEnumerator = typeof(IEnumerable<>)
                .MakeGenericType(item.Type)
                .GetMethod("GetEnumerator", Type.EmptyTypes);

            var moveNext = typeof(IEnumerator)
                .GetMethod(nameof(IEnumerator.MoveNext), Type.EmptyTypes);

            var toList = typeof(Enumerable)
                .GetMethod(nameof(Enumerable.ToList))
                .MakeGenericMethod(item.Type);

            return Expression.Block(
                new[] { enumerator, collection },
                Expression.Assign(
                    collection,
                    Expression.Condition(
                        Expression.TypeIs(items, collection.Type),
                        Expression.Convert(items, collection.Type),
                        Expression.Convert(Expression.Call(null, toList, Expression.Convert(items, enumerable.Type)), collection.Type))),
                Expression.IfThen(
                    Expression.GreaterThan(Expression.Property(collection, getCount), Expression.Constant(0)),
                    Expression.Block(
                        WriteInteger(Expression.Property(collection, getCount), stream),
                        Expression.Assign(enumerator, Expression.Call(collection, getEnumerator)),
                        Expression.TryFinally(
                            Expression.Loop(
                                Expression.IfThenElse(
                                    Expression.Call(enumerator, moveNext),
                                    Expression.Block(
                                        new[] { item },
                                        Expression.Assign(item, Expression.Property(enumerator, getCurrent)),
                                        writeItem),
                                    Expression.Break(loop)),
                                loop),
                            Expression.Call(enumerator, dispose)))),
                WriteInteger(Expression.Constant(0L), stream));
        }
        /// <summary>
        /// Generates an expression that writes a boolean.
        /// </summary>
        public virtual Expression WriteBoolean(Expression value, Expression stream)
        {
            var write = typeof(Stream)
                .GetMethod(nameof(Stream.WriteByte), new[] { typeof(byte) });

            var @false = Expression.Constant((byte)0);
            var @true = Expression.Constant((byte)1);

            return Expression.Call(stream, write, Expression.Condition(value, @true, @false));
        }

        /// <summary>
        /// Generates an expression that writes a single- or double-precision floating-point number.
        /// </summary>
        public virtual Expression WriteFloat(Expression value, Expression stream)
        {
            var bytes = Expression.Variable(typeof(byte[]));

            var convert = typeof(BitConverter)
                .GetMethod(nameof(BitConverter.GetBytes), new[] { value.Type });

            var reverse = typeof(Array)
                .GetMethod(nameof(Array.Reverse), new[] { convert.ReturnType });

            return Expression.Block(
                new[] { bytes },
                Expression.Assign(bytes, Expression.Call(null, convert, value)),
                BitConverter.IsLittleEndian
                    ? Expression.Empty() as Expression
                    : Expression.Call(null, reverse, bytes) as Expression,
                Write(bytes, stream));
        }

        /// <summary>
        /// Generates an expression that writes a zig-zag encoded integer.
        /// </summary>
        public virtual Expression WriteInteger(Expression value, Expression stream)
        {
            var chunk = Expression.Variable(typeof(ulong));
            var encoded = Expression.Variable(typeof(ulong));

            var loop = Expression.Label();

            var write = typeof(Stream)
                .GetMethod(nameof(Stream.WriteByte), new[] { typeof(byte) });

            return Expression.Block(
                new[] { chunk, encoded },
                Expression.Assign(
                    encoded,
                    Expression.Convert(
                        Expression.ExclusiveOr(
                            Expression.LeftShift(value, Expression.Constant(1)),
                            Expression.RightShift(value, Expression.Constant(63))),
                        encoded.Type)),
                Expression.Loop(
                    Expression.Block(
                        Expression.Assign(chunk, Expression.And(encoded, Expression.Constant(0x7fUL))),
                        Expression.RightShiftAssign(encoded, Expression.Constant(7)),
                        Expression.IfThen(
                            Expression.NotEqual(encoded, Expression.Constant(0UL)),
                            Expression.OrAssign(chunk, Expression.Constant(0x80UL))),
                        Expression.Call(stream, write, Expression.Convert(chunk, typeof(byte))),
                        Expression.IfThen(
                            Expression.Equal(encoded, Expression.Constant(0UL)),
                            Expression.Break(loop))),
                    loop));
        }

        /// <summary>
        /// Generates an expression that writes key-value blocks to a stream.
        /// </summary>
        public Expression WriteMap(Expression pairs, ParameterExpression key, ParameterExpression value, Expression writeKey, Expression writeValue, Expression stream)
        {
            var pair = Expression.Variable(typeof(KeyValuePair<,>).MakeGenericType(key.Type, value.Type));

            var getKey = pair.Type
                .GetProperty("Key")
                .GetGetMethod();

            var getValue = pair.Type
                .GetProperty("Value")
                .GetGetMethod();

            return WriteArray(
                pairs,
                pair,
                Expression.Block(
                    new[] { key, value },
                    Expression.Assign(key, Expression.Property(pair, getKey)),
                    Expression.Assign(value, Expression.Property(pair, getValue)),
                    writeKey,
                    writeValue),
                stream);
        }
    }
}
