namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="IntSchema" />.
    /// </summary>
    public abstract class IntSerializerBuilderCase : SerializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions from native integer
        /// types. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target == typeof(int))
            {
                var toInt32 = typeof(IntPtr)
                    .GetMethod(nameof(IntPtr.ToInt32), Type.EmptyTypes);

                var toUInt32 = typeof(UIntPtr)
                    .GetMethod(nameof(UIntPtr.ToUInt32), Type.EmptyTypes);

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
                                toInt32))),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(UIntPtr)),
                        Expression.Return(
                            result,
                            base.BuildDynamicConversion(
                                Expression.Call(
                                    Expression.Convert(intermediate, typeof(UIntPtr)),
                                    toUInt32),
                                typeof(int)))),
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
            if (target == typeof(int))
            {
                if (value.Type == typeof(IntPtr))
                {
                    var toInt32 = typeof(IntPtr)
                        .GetMethod(nameof(IntPtr.ToInt32), Type.EmptyTypes);

                    value = Expression.Call(value, toInt32);
                }
                else if (value.Type == typeof(UIntPtr))
                {
                    var toUInt32 = typeof(UIntPtr)
                        .GetMethod(nameof(UIntPtr.ToUInt32), Type.EmptyTypes);

                    value = Expression.Call(value, toUInt32);
                }
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
