namespace Chr.Avro.Abstract
{
    /// <summary>
    /// Wraps the default value of a <see cref="RecordField" /> represented as a .NET object.
    /// </summary>
    /// <typeparam name="TValue">
    /// The type of the underlying .NET value.
    /// </typeparam>
    public class ObjectDefaultValue<TValue> : DefaultValue
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectDefaultValue{TValue}" /> class.
        /// </summary>
        /// <param name="value">
        /// The value represented as an <see cref="object" />.
        /// </param>
        /// <param name="schema">
        /// A <see cref="Schema" /> that can be used to read the value.
        /// </param>
        public ObjectDefaultValue(TValue value, Schema schema)
            : base(schema)
        {
            Value = value;
        }

        /// <summary>
        /// Gets or sets the <see cref="object" /> representation of the value.
        /// </summary>
        public TValue Value { get; set; }

        /// <inheritdoc />
        /// <exception cref="UnsupportedTypeException">
        /// Thrown when the underlying object cannot be cast to <typeparamref name="T" />.
        /// </exception>
        public override T? ToObject<T>()
            where T : default
        {
            object? boxed = Value;

            if (typeof(T).IsAssignableFrom(boxed?.GetType() ?? typeof(TValue)))
            {
                return (T?)boxed;
            }
            else
            {
                throw new UnsupportedTypeException(typeof(T), $"Default value of type {typeof(TValue)} cannot be used as type {typeof(T)}.");
            }
        }
    }
}
