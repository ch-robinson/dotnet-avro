namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="IntSchema" />.
    /// </summary>
    public abstract class IntDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target == typeof(IntPtr))
            {
                var constructor = target
                    .GetConstructor(new[] { typeof(int) });

                return Expression.New(
                    constructor,
                    Expression.ConvertChecked(value, typeof(int)));
            }
            else if (target == typeof(UIntPtr))
            {
                var constructor = target
                    .GetConstructor(new[] { typeof(uint) });

                return Expression.New(
                    constructor,
                    Expression.ConvertChecked(value, typeof(uint)));
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
