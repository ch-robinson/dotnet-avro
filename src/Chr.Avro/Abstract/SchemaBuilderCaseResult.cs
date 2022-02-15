namespace Chr.Avro.Abstract
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents the outcome of a <see cref="ISchemaBuilderCase" />.
    /// </summary>
    public class SchemaBuilderCaseResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaBuilderCaseResult" /> class.
        /// </summary>
        public SchemaBuilderCaseResult()
        {
            Exceptions = new List<Exception>();
        }

        /// <summary>
        /// Gets or sets <see cref="Exception" />s related to the inapplicability of the case. If
        /// <see cref="Schema" /> is not <c>null</c>, these exceptions should be interpreted as
        /// warnings.
        /// </summary>
        public virtual ICollection<Exception> Exceptions { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="Abstract.Schema" /> built by applying the case. If <c>null</c>,
        /// the case was not applied successfully.
        /// </summary>
        public virtual Schema? Schema { get; set; }

        /// <summary>
        /// Creates a new <see cref="SchemaBuilderCaseResult" /> for an unsuccessful outcome.
        /// </summary>
        /// <param name="exception">
        /// An <see cref="Exception" /> describing the inapplicability of the case.
        /// </param>
        /// <returns>
        /// A <see cref="SchemaBuilderCaseResult" /> with <see cref="Exceptions" /> populated and
        /// <see cref="Schema" /> <c>null</c>.
        /// </returns>
        public static SchemaBuilderCaseResult FromException(Exception exception)
        {
            var result = new SchemaBuilderCaseResult();
            result.Exceptions.Add(exception);

            return result;
        }

        /// <summary>
        /// Creates a new <see cref="SchemaBuilderCaseResult" /> for an unsuccessful outcome.
        /// </summary>
        /// <param name="schema">
        /// The <see cref="Schema" /> built by applying the case.
        /// </param>
        /// <returns>
        /// A <see cref="SchemaBuilderCaseResult" /> with <see cref="Schema" /> populated.
        /// </returns>
        public static SchemaBuilderCaseResult FromSchema(Schema schema)
        {
            return new SchemaBuilderCaseResult
            {
                Schema = schema,
            };
        }
    }
}
