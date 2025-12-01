namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Text.Json;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Represents the state of a <see cref="JsonDeserializerBuilder" /> operation.
    /// </summary>
    public class JsonDeserializerBuilderContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDeserializerBuilderContext" /> class.
        /// </summary>
        /// <param name="reader">
        /// An <see cref="Expression" /> representing the input <see cref="Utf8JsonReader" />. If a
        /// <see cref="ParameterExpression" /> is not provided, one will be created.
        /// </param>
        public JsonDeserializerBuilderContext(ParameterExpression? reader = null)
        {
            Assignments = new Dictionary<ParameterExpression, Expression>();
            References = new Dictionary<(Schema, Type), ParameterExpression>();
            Reader = reader ?? Expression.Parameter(typeof(Utf8JsonReader).MakeByRefType());
            RecursiveReferences = new Dictionary<Schema, bool>();
        }

        /// <summary>
        /// Gets a map of top-level variables to their values. Each <see cref="ParameterExpression" />
        /// will be assigned to its corresponding <see cref="Expression" /> at the top level of the
        /// generated <see cref="JsonDeserializer{T}" />;. Every value in <see cref="References" />
        /// should be a key in <see cref="Assignments" />.
        /// </summary>
        public virtual IDictionary<ParameterExpression, Expression> Assignments { get; }

        /// <summary>
        /// Gets the expression that represents the <see cref="Utf8JsonReader" /> argument of
        /// <see cref="JsonDeserializer{T}" />.
        /// </summary>
        public virtual ParameterExpression Reader { get; }

        /// <summary>
        /// Gets a map of <see cref="Schema" />-<see cref="Type" /> pairs to top-level
        /// <see cref="ParameterExpression" />s. If a <see cref="ParameterExpression" /> is present
        /// for a specific pair, that <see cref="ParameterExpression" /> will be returned by the
        /// <see cref="JsonDeserializerBuilder" /> for all subsequent occurrences of the pair.
        /// This is necessary for potentially recursive deserializers, such as ones built for
        /// <see cref="RecordSchema" />s.
        /// </summary>
        public virtual IDictionary<(Schema Schema, Type Type), ParameterExpression> References { get; }

        /// <summary>
        /// Gets a map that associates whether a <see cref="Schema"/> is on any potentially recursive path.
        /// A value of true means the schema is part of a recursive path, false means it is not.
        /// </summary>
        internal IDictionary<Schema, bool> RecursiveReferences { get; }
    }
}
