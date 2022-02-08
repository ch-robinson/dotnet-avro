namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="FixedSchema" />.
    /// </summary>
    public abstract class FixedSerializerBuilderCase : SerializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions to types that can be
        /// idiomatically represented as byte arrays. If none match, the base implementation is
        /// used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type intermediate)
        {
            if (value.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                value = Expression.Call(value, convertGuid);
            }

            return base.BuildStaticConversion(value, intermediate);
        }
    }
}
