namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="LongSchema" />.
    /// </summary>
    public abstract class LongDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target == typeof(IntPtr))
            {
                var constructor = target
                    .GetConstructor(new[] { typeof(long) });

                return Expression.New(
                    constructor,
                    Expression.ConvertChecked(value, typeof(long)));
            }
            else if (target == typeof(UIntPtr))
            {
                var constructor = target
                    .GetConstructor(new[] { typeof(ulong) });

                return Expression.New(
                    constructor,
                    Expression.ConvertChecked(value, typeof(ulong)));
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
