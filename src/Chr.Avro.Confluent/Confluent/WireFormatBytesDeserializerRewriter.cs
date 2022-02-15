namespace Chr.Avro.Confluent
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Serialization;

    /// <summary>
    /// Rewrites generated deserializers for <see cref="BytesSchema" />s to conform to the "raw
    /// bytes" case of the Confluent wire format.
    /// </summary>
    internal class WireFormatBytesDeserializerRewriter : ExpressionVisitor
    {
        private readonly ParameterExpression span;
        private bool isRewriteComplete;

        /// <summary>
        /// Initializes a new instance of the <see cref="WireFormatBytesDeserializerRewriter" />
        /// class.
        /// </summary>
        /// <param name="span">
        /// A <see cref="ParameterExpression" /> representing the data being deserialized.
        /// </param>
        public WireFormatBytesDeserializerRewriter(ParameterExpression span)
        {
            this.span = span;
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
            var readBytes = typeof(BinaryReader)
                .GetMethod(nameof(BinaryReader.ReadBytes), Type.EmptyTypes);

            if (node.Method == readBytes)
            {
                isRewriteComplete = true;

                var toArray = span.Type
                    .GetMethod(nameof(ReadOnlySpan<byte>.ToArray), Type.EmptyTypes);

                return Expression.Call(span, toArray);
            }

            return base.VisitMethodCall(node);
        }
    }
}
