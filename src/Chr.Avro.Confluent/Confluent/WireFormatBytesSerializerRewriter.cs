namespace Chr.Avro.Confluent
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    using Stream = System.IO.Stream;

    /// <summary>
    /// Rewrites generated serializers for <see cref="BytesSchema" />s to conform to the "raw
    /// bytes" case of the Confluent wire format.
    /// </summary>
    internal class WireFormatBytesSerializerRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression stream;
        private bool isRewriteComplete;

        /// <summary>
        /// Initializes a new instance of the <see cref="WireFormatBytesSerializerRewriter" />
        /// class.
        /// </summary>
        /// <param name="stream">
        /// A <see cref="ParameterExpression" /> representing the stream being serialized to.
        /// </param>
        public WireFormatBytesSerializerRewriter(ParameterExpression stream)
        {
            this.stream = stream;
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
            var readBytes = typeof(BinaryWriter)
                .GetMethod(nameof(BinaryWriter.WriteBytes), new[] { typeof(byte[]) });

            if (node.Method == readBytes)
            {
                isRewriteComplete = true;

                var write = stream.Type
                    .GetMethod(nameof(Stream.Write), new[] { typeof(byte[]), typeof(int), typeof(int) });

                return Expression.Call(
                    stream,
                    write,
                    node.Arguments[0],
                    Expression.Constant(0),
                    Expression.ArrayLength(node.Arguments[0]));
            }

            return base.VisitMethodCall(node);
        }
    }
}
