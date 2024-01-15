namespace Chr.Avro.Serialization
{
    using System;
    using System.Globalization;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Xml;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for deserializer builder cases that match <see cref="StringSchema" />.
    /// </summary>
    public class StringDeserializerBuilderCase : DeserializerBuilderCase
    {
        /// <remarks>
        /// This override includes additional conditions to handle conversions to types that can be
        /// idiomatically represented as strings. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target == typeof(DateTime) || target == typeof(DateTime?))
            {
                var parseDateTime = typeof(DateTime)
                    .GetMethod(nameof(DateTime.Parse), new[] { value.Type, typeof(IFormatProvider), typeof(DateTimeStyles) });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateTime,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture),
                        Expression.Constant(DateTimeStyles.RoundtripKind)),
                    target);
            }
            else if (target == typeof(DateTimeOffset) || target == typeof(DateTimeOffset?))
            {
                var parseDateTimeOffset = typeof(DateTimeOffset)
                    .GetMethod(nameof(DateTimeOffset.Parse), new[] { value.Type, typeof(IFormatProvider), typeof(DateTimeStyles) });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateTimeOffset,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture),
                        Expression.Constant(DateTimeStyles.RoundtripKind)),
                    target);
            }
#if NET6_0_OR_GREATER
            else if (target == typeof(DateOnly) || target == typeof(DateOnly?))
            {
                var parseDateOnly = typeof(DateOnly)
                    .GetMethod(nameof(DateOnly.Parse), new[] { value.Type, typeof(IFormatProvider) });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateOnly,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture)),
                    target);
            }
            else if (target == typeof(TimeOnly) || target == typeof(TimeOnly?))
            {
                var parseTimeOnly = typeof(TimeOnly)
                    .GetMethod(nameof(TimeOnly.Parse), new[] { value.Type, typeof(IFormatProvider) });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseTimeOnly,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture)),
                    target);
            }
#endif
            else if (target.IsEnum)
            {
                var cases = target.GetEnumMembers()
                    .Select(field => Expression.SwitchCase(
                        Expression.Constant(Enum.Parse(target, field.Name)),
                        Expression.Constant(field.GetEnumMemberName())));

                var exceptionConstructor = typeof(ArgumentException)
                    .GetConstructor(new[] { typeof(string) });

                value = Expression.Switch(
                    value,
                    Expression.Throw(
                        Expression.New(
                            exceptionConstructor,
                            Expression.Constant($"Invalid enum symbol.")),
                        target),
                    cases.ToArray());
            }
            else if (target == typeof(Guid) || target == typeof(Guid?))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(guidConstructor, value);
            }
            else if (target == typeof(TimeSpan) || target == typeof(TimeSpan?))
            {
                var parseTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToTimeSpan));

                value = Expression.Call(null, parseTimeSpan, value);
            }
            else if (target == typeof(Uri))
            {
                var uriConstructor = typeof(Uri)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(uriConstructor, value);
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
