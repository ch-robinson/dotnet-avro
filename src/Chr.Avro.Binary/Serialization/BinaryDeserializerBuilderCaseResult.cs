namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Represents the outcome of a
    /// <see cref="IDeserializerBuilderCase{BinaryDeserializerBuilderContext, BinaryDeserializerBuilderCaseResult}" />.
    /// </summary>
    public class BinaryDeserializerBuilderCaseResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BinaryDeserializerBuilderCaseResult" /> class.
        /// </summary>
        public BinaryDeserializerBuilderCaseResult()
        {
            Exceptions = new List<Exception>();
        }

        /// <summary>
        /// Gets or sets exceptions regarding the inapplicability of the case. If <see cref="Expression" />
        /// is not <c>null</c>, these exceptions should be interpreted as warnings.
        /// </summary>
        public virtual ICollection<Exception> Exceptions { get; set; }

        /// <summary>
        /// Gets or sets the expression obtained by applying the case. If <c>null</c>, the case was
        /// not applied successfully.
        /// </summary>
        public virtual Expression? Expression { get; set; }

        /// <summary>
        /// Creates a new <see cref="BinaryDeserializerBuilderCaseResult" /> for an unsuccessful
        /// outcome.
        /// </summary>
        /// <param name="exception">
        /// An exception describing the inapplicability of the case.
        /// </param>
        /// <returns>
        /// A <see cref="BinaryDeserializerBuilderCaseResult" /> with <see cref="Exceptions" />
        /// populated and <see cref="Expression" /> <c>null</c>.
        /// </returns>
        public static BinaryDeserializerBuilderCaseResult FromException(Exception exception)
        {
            var result = new BinaryDeserializerBuilderCaseResult();
            result.Exceptions.Add(exception);

            return result;
        }

        /// <summary>
        /// Creates a new <see cref="BinaryDeserializerBuilderCaseResult" /> for an unsuccessful
        /// outcome.
        /// </summary>
        /// <param name="expression">
        /// The expression obtained by applying the case.
        /// </param>
        /// <returns>
        /// A <see cref="BinaryDeserializerBuilderCaseResult" /> with <see cref="Expression" />
        /// populated.
        /// </returns>
        public static BinaryDeserializerBuilderCaseResult FromExpression(Expression expression)
        {
            return new BinaryDeserializerBuilderCaseResult
            {
                Expression = expression,
            };
        }
    }
}
