namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="LongSchema" />.
    /// </summary>
    public abstract class LongSerializerBuilderCase : SerializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions from native integer
        /// types. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target == typeof(long))
            {
                var toInt64 = typeof(IntPtr)
                    .GetMethod(nameof(IntPtr.ToInt64), Type.EmptyTypes);

                var toUInt64 = typeof(UIntPtr)
                    .GetMethod(nameof(UIntPtr.ToUInt64), Type.EmptyTypes);

                var intermediate = Expression.Variable(value.Type);
                var result = Expression.Label(target);

                return Expression.Block(
                    new[] { intermediate },
                    Expression.Assign(intermediate, value),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(IntPtr)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(IntPtr)),
                                toInt64))),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(UIntPtr)),
                        Expression.Return(
                            result,
                            base.BuildDynamicConversion(
                                Expression.Call(
                                    Expression.Convert(intermediate, typeof(UIntPtr)),
                                    toUInt64),
                                typeof(long)))),
                    Expression.Label(result, base.BuildDynamicConversion(intermediate, target)));
            }

            return base.BuildDynamicConversion(value, target);
        }

        /// <remarks>
        /// This override includes additional conditions to handle conversions from native integer
        /// types. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target == typeof(long))
            {
                if (value.Type == typeof(IntPtr))
                {
                    var toInt64 = typeof(IntPtr)
                        .GetMethod(nameof(IntPtr.ToInt64), Type.EmptyTypes);

                    value = Expression.Call(value, toInt64);
                }
                else if (value.Type == typeof(UIntPtr))
                {
                    var toUInt64 = typeof(UIntPtr)
                        .GetMethod(nameof(UIntPtr.ToUInt64), Type.EmptyTypes);

                    value = Expression.Call(value, toUInt64);
                }
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
