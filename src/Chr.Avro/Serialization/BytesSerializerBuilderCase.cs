namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="BytesSchema" />.
    /// </summary>
    public abstract class BytesSerializerBuilderCase : SerializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions from types that can
        /// be idiomatically represented as byte arrays. If none match, the base implementation is
        /// used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildConversion(Expression value, Type intermediate)
        {
            if (value.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                value = Expression.Call(value, convertGuid);
            }

            return base.BuildConversion(value, intermediate);
        }
    }
}
