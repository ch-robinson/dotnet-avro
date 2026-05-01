namespace Chr.Avro.Confluent
{
    using System;
    using System.Buffers;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    /// <summary>
    /// Rewrites generated serializers for <see cref="BytesSchema" />s to conform to the "raw
    /// bytes" case of the Confluent wire format.
    /// </summary>
    internal class WireFormatBytesSerializerRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression output;
        private bool isRewriteComplete;

        /// <summary>
        /// Initializes a new instance of the <see cref="WireFormatBytesSerializerRewriter" />
        /// class.
        /// </summary>
        /// <param name="output">
        /// A <see cref="ParameterExpression" /> representing the <see cref="IBufferWriter{T}" />
        /// being serialized to.
        /// </param>
        public WireFormatBytesSerializerRewriter(ParameterExpression output)
        {
            this.output = output;
        }

        /// <summary>
        /// Writes a byte array directly to an <see cref="IBufferWriter{T}" />.
        /// Called via expression trees to avoid ref-struct limitations.
        /// </summary>
        public static void WriteToBufferWriter(IBufferWriter<byte> output, byte[] data)
        {
            var span = output.GetSpan(data.Length);
            new ReadOnlySpan<byte>(data).CopyTo(span);
            output.Advance(data.Length);
        }

        /// <inheritdoc />
        public override Expression Visit(Expression node)
        {
            if (isRewriteComplete)
            {
                return node;
            }

            return base.Visit(node);
        }

        /// <inheritdoc />
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var writeBytes = typeof(BinaryWriter)
                .GetMethod(nameof(BinaryWriter.WriteBytes), new[] { typeof(byte[]) });

            if (node.Method == writeBytes)
            {
                isRewriteComplete = true;

                var writeToBufferWriter = typeof(WireFormatBytesSerializerRewriter)
                    .GetMethod(nameof(WriteToBufferWriter), new[] { typeof(IBufferWriter<byte>), typeof(byte[]) });

                return Expression.Call(
                    writeToBufferWriter,
                    output,
                    node.Arguments[0]);
            }

            return base.VisitMethodCall(node);
        }
    }
}
