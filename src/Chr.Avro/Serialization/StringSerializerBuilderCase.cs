namespace Chr.Avro.Serialization
{
    using System;
    using System.Globalization;
    using System.Linq.Expressions;
    using System.Xml;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="StringSchema" />.
    /// </summary>
    public class StringSerializerBuilderCase : SerializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions to types that can be
        /// idiomatically represented as strings. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildConversion(Expression value, Type intermediate)
        {
            if (value.Type == typeof(DateTime))
            {
                var convertDateTime = typeof(DateTime)
                    .GetMethod(nameof(DateTime.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                value = Expression.Call(
                    value,
                    convertDateTime,
                    Expression.Constant("O"),
                    Expression.Constant(CultureInfo.InvariantCulture));
            }
            else if (value.Type == typeof(DateTimeOffset))
            {
                var convertDateTimeOffset = typeof(DateTimeOffset)
                    .GetMethod(nameof(DateTimeOffset.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                value = Expression.Call(
                    value,
                    convertDateTimeOffset,
                    Expression.Constant("O"),
                    Expression.Constant(CultureInfo.InvariantCulture));
            }
            else if (value.Type == typeof(Guid))
            {
                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToString), Type.EmptyTypes);

                value = Expression.Call(value, convertGuid);
            }
            else if (value.Type == typeof(TimeSpan))
            {
                var convertTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToString), new[] { typeof(TimeSpan) });

                value = Expression.Call(null, convertTimeSpan, value);
            }
            else if (value.Type == typeof(Uri))
            {
                var convertUri = typeof(Uri)
                    .GetMethod(nameof(Uri.ToString));

                value = Expression.Call(value, convertUri);
            }

            return base.BuildConversion(value, intermediate);
        }
    }
}
