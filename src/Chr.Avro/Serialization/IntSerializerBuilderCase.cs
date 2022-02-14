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
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (value.Type == typeof(IntPtr))
            {
                var toInt32 = value.Type
                    .GetMethod(nameof(IntPtr.ToInt32), Type.EmptyTypes);

                value = Expression.Call(value, toInt32);
            }
            else if (value.Type == typeof(UIntPtr))
            {
                var toUInt32 = value.Type
                    .GetMethod(nameof(UIntPtr.ToUInt32), Type.EmptyTypes);

                value = Expression.Call(value, toUInt32);
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
