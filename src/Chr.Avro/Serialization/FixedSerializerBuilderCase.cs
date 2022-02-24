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
        /// This override includes additional conditions to handle conversions from types that can
        /// be idiomatically represented as byte arrays. If none match, the base implementation is
        /// used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target == typeof(byte[]))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                var intermediate = Expression.Variable(value.Type);
                var result = Expression.Label(target);

                return Expression.Block(
                    new[] { intermediate },
                    Expression.Assign(intermediate, value),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(Guid)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(Guid)),
                                convertGuid))),
                    Expression.Label(result, base.BuildDynamicConversion(intermediate, target)));
            }
            else
            {
                return base.BuildDynamicConversion(value, target);
            }
        }

        /// <remarks>
        /// This override includes additional conditions to handle conversions from types that can
        /// be idiomatically represented as byte arrays. If none match, the base implementation is
        /// used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target == typeof(byte[]))
            {
                if (value.Type == typeof(Guid))
                {
                    var convertGuid = typeof(Guid)
                        .GetMethod(nameof(Guid.ToByteArray), Type.EmptyTypes);

                    value = Expression.Call(value, convertGuid);
                }
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
