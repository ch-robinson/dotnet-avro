namespace Chr.Avro.Abstract
{
    using System.Text;
    using System.Text.Json;
    using Chr.Avro.Serialization;

    /// <summary>
    /// Wraps the default value of a <see cref="RecordField" /> represented as JSON.
    /// </summary>
    public class JsonDefaultValue : DefaultValue
    {
        private readonly IJsonDeserializerBuilder deserializerBuilder;

        private JsonElement element;

        /// <summary>
        /// Initializes a new instance of the <see cref="JsonDefaultValue" /> class.
        /// </summary>
        /// <param name="element">
        /// The value represented as a <see cref="JsonElement" />.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> that can be used to read the value.
        /// </param>
        /// <param name="deserializerBuilder">
        /// A deserializer builder instance to use when deserializing the value to .NET objects. If
        /// none is provided, the default <see cref="JsonDeserializerBuilder" /> will be used.
        /// </param>
        public JsonDefaultValue(JsonElement element, Schema schema, IJsonDeserializerBuilder? deserializerBuilder = default)
            : base(schema)
        {
            Element = element;

            this.deserializerBuilder = deserializerBuilder ?? new JsonDeserializerBuilder();
        }

        /// <summary>
        /// Gets or sets the <see cref="JsonElement" /> representation of the value.
        /// </summary>
        public JsonElement Element
        {
            get
            {
                return element;
            }

            set
            {
                // A new element is returned only if the element (or its parent) is not the result
                // of a previous call to .Clone:
                element = value.Clone();
            }
        }

        /// <inheritdoc />
        public override T? ToObject<T>()
            where T : default
        {
            var reader = ToJsonReader();

            return deserializerBuilder.BuildDelegate<T>(Schema)(ref reader);
        }

        private Utf8JsonReader ToJsonReader()
        {
            return new Utf8JsonReader(Encoding.UTF8.GetBytes(Element.GetRawText()));
        }
    }
}
