namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Represents the state of a <see cref="JsonSerializerBuilder" /> operation.
    /// </summary>
    public class JsonSerializerBuilderContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonSerializerBuilderContext" /> class.
        /// </summary>
        /// <param name="writer">
        /// An <see cref="Expression" /> representing the output <see cref="Utf8JsonWriter" />. If
        /// a <see cref="ParameterExpression" /> is not provided, one will be created.
        /// </param>
        public JsonSerializerBuilderContext(ParameterExpression? writer = null)
        {
            Assignments = new Dictionary<ParameterExpression, Expression>();
            References = new Dictionary<(Schema, Type), ParameterExpression>();
            Writer = writer ?? Expression.Parameter(typeof(Utf8JsonWriter));
        }

        /// <summary>
        /// Gets a map of top-level variables to their values. Each <see cref="ParameterExpression" />
        /// will be assigned to its corresponding <see cref="Expression" /> at the top level of the
        /// generated <see cref="JsonSerializer{T}" />;. Every value in <see cref="References" />
        /// should be a key in <see cref="Assignments" />.
        /// </summary>
        public virtual IDictionary<ParameterExpression, Expression> Assignments { get; }

        /// <summary>
        /// Gets a map of <see cref="Schema" />-<see cref="Type" /> pairs to top-level
        /// <see cref="ParameterExpression" />s. If a <see cref="ParameterExpression" /> is present
        /// for a specific pair, that <see cref="ParameterExpression" /> will be returned by the
        /// <see cref="JsonSerializerBuilder" /> for all subsequent occurrences of the pair. This
        /// is necessary for potentially recursive serializers, such as ones built for
        /// <see cref="RecordSchema" />s.
        /// </summary>
        public virtual IDictionary<(Schema, Type), ParameterExpression> References { get; }

        /// <summary>
        /// Gets the expression that represents the <see cref="Utf8JsonWriter" /> argument of
        /// <see cref="JsonSerializer{T}" />.
        /// </summary>
        public ParameterExpression Writer { get; }
    }
}
