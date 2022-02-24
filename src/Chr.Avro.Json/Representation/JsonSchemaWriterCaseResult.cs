namespace Chr.Avro.Representation
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents the outcome of a
    /// <see cref="ISchemaWriterCase{JsonSchemaWriterContext, JsonSchemaWriterCaseResult}" />.
    /// </summary>
    public class JsonSchemaWriterCaseResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSchemaWriterCaseResult" /> class.
        /// </summary>
        public JsonSchemaWriterCaseResult()
        {
            Exceptions = new List<Exception>();
        }

        /// <summary>
        /// Gets or sets <see cref="Exception" />s related to the inapplicability of the case.
        /// </summary>
        public virtual ICollection<Exception> Exceptions { get; set; }

        /// <summary>
        /// Creates a new <see cref="JsonSchemaWriterCaseResult" /> for an unsuccessful outcome.
        /// </summary>
        /// <param name="exception">
        /// An <see cref="Exception" /> describing the inapplicability of the case.
        /// </param>
        /// <returns>
        /// A <see cref="JsonSchemaWriterCaseResult" /> with <see cref="Exceptions" /> populated.
        /// </returns>
        public static JsonSchemaWriterCaseResult FromException(Exception exception)
        {
            var result = new JsonSchemaWriterCaseResult();
            result.Exceptions.Add(exception);

            return result;
        }
    }
}
