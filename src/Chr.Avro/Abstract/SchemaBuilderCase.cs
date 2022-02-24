namespace Chr.Avro.Abstract
{
    using System;

    /// <summary>
    /// Provides a base schema builder case implementation.
    /// </summary>
    public abstract class SchemaBuilderCase
    {
        /// <summary>
        /// The <see cref="Type" /> to use as the nullable key.
        /// </summary>
        protected static readonly Type NullableType = typeof(Nullable<>);
    }
}
