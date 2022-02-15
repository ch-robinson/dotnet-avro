namespace Chr.Avro.Confluent
{
    /// <summary>
    /// Options for automatic schema registration by Schema Registry serializers.
    /// </summary>
    public enum AutomaticRegistrationBehavior
    {
        /// <summary>
        /// Never register schemas automatically; fail to build a serializer when the subject does
        /// not exist or the latest version is not compatible with the type.
        /// </summary>
        Never,

        /// <summary>
        /// Always register schemas automatically; ensure that the latest version of the subject
        /// matches the generated schema for the type.
        /// </summary>
        Always,
    }
}
