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
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (value.Type == typeof(IntPtr))
            {
                var toInt64 = value.Type
                    .GetMethod(nameof(IntPtr.ToInt64), Type.EmptyTypes);

                value = Expression.Call(value, toInt64);
            }
            else if (value.Type == typeof(UIntPtr))
            {
                var toUInt64 = value.Type
                    .GetMethod(nameof(UIntPtr.ToUInt64), Type.EmptyTypes);

                value = Expression.Call(value, toUInt64);
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
