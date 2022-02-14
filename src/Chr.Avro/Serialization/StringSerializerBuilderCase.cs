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
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target == typeof(string))
            {
                var convertDateTime = typeof(DateTime)
                    .GetMethod(nameof(DateTime.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                var convertDateTimeOffset = typeof(DateTimeOffset)
                    .GetMethod(nameof(DateTimeOffset.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                var convertGuid = typeof(Guid)
                    .GetMethod(nameof(Guid.ToString), Type.EmptyTypes);

                var convertTimeSpan = typeof(XmlConvert)
                    .GetMethod(nameof(XmlConvert.ToString), new[] { typeof(TimeSpan) });

                var getType = typeof(object)
                    .GetMethod(nameof(object.GetType));

                var isEnum = typeof(Type)
                    .GetProperty(nameof(Type.IsEnum));

                var toString = typeof(object)
                    .GetMethod(nameof(object.ToString), Type.EmptyTypes);

                var intermediate = Expression.Variable(value.Type);
                var result = Expression.Label(target);

                return Expression.Block(
                    new[] { intermediate },
                    Expression.Assign(intermediate, value),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(DateTime)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(DateTime)),
                                convertDateTime,
                                Expression.Constant("O"),
                                Expression.Constant(CultureInfo.InvariantCulture)))),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(DateTimeOffset)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(DateTimeOffset)),
                                convertDateTimeOffset,
                                Expression.Constant("O"),
                                Expression.Constant(CultureInfo.InvariantCulture)))),
                    Expression.IfThen(
                        Expression.Property(Expression.Call(intermediate, getType), isEnum),
                        Expression.Return(
                            result,
                            Expression.Call(
                                intermediate,
                                toString))),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(Guid)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(Guid)),
                                convertGuid))),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(TimeSpan)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                null,
                                convertTimeSpan,
                                Expression.Convert(intermediate, typeof(TimeSpan))))),
                    Expression.Label(result, base.BuildDynamicConversion(intermediate, target)));
            }
            else
            {
                return base.BuildDynamicConversion(value, target);
            }
        }

        /// <remarks>
        /// This override includes additional conditions to handle conversions to types that can be
        /// idiomatically represented as strings. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target == typeof(string))
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
                else if (value.Type.IsEnum)
                {
                    var convertEnum = typeof(Enum)
                        .GetMethod(nameof(Enum.ToString), Type.EmptyTypes);

                    value = Expression.Call(value, convertEnum);
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
            }

            return base.BuildStaticConversion(value, target);
        }
    }
}
