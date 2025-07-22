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
            var underlying = Nullable.GetUnderlyingType(target) ?? target;

            if (underlying == typeof(DateTime))
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
            else if (underlying == typeof(DateTimeOffset))
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
            else if (underlying == typeof(DateOnly))
            {
                var parseDateOnly = typeof(DateOnly)
                    .GetMethod(nameof(DateOnly.Parse), new[]
                    {
                        value.Type,
                        typeof(IFormatProvider),
                        typeof(DateTimeStyles)
                    });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseDateOnly,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture),
                        Expression.Constant(DateTimeStyles.None)),
                    target);
            }
            else if (underlying == typeof(TimeOnly))
            {
                var parseTimeOnly = typeof(TimeOnly)
                    .GetMethod(nameof(TimeOnly.Parse), new[]
                    {
                        value.Type,
                        typeof(IFormatProvider),
                        typeof(DateTimeStyles)
                    });

                value = Expression.ConvertChecked(
                    Expression.Call(
                        null,
                        parseTimeOnly,
                        value,
                        Expression.Constant(CultureInfo.InvariantCulture),
                        Expression.Constant(DateTimeStyles.None)),
                    target);
            }
#endif
            else if (underlying.IsEnum)
            {
                var cases = underlying.GetEnumMembers()
                    .Select(field => Expression.SwitchCase(
                        Expression.Convert(Expression.Constant(Enum.Parse(underlying, field.Name)), target),
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
            else if (underlying == typeof(Guid))
            {
                var guidConstructor = typeof(Guid)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(guidConstructor, value);
            }
            else if (underlying == typeof(TimeSpan))
            {
                var parseTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToTimeSpan));

                value = Expression.Call(null, parseTimeSpan, value);
            }
            else if (underlying == typeof(Uri))
            {
                var uriConstructor = typeof(Uri)
                    .GetConstructor(new[] { value.Type });

                value = Expression.New(uriConstructor, value);
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
