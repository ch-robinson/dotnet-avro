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
#if NET6_0_OR_GREATER

                var convertDateOnly = typeof(DateOnly)
                    .GetMethod(nameof(DateOnly.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                var convertTimeOnly = typeof(TimeOnly)
                    .GetMethod(nameof(TimeOnly.ToString), new[] { typeof(string), typeof(IFormatProvider) });
#endif

                var convertEnum = typeof(ReflectionExtensions)
                    .GetMethod(nameof(ReflectionExtensions.GetEnumMemberName), new[] { typeof(Type), typeof(object) });

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
#if NET6_0_OR_GREATER
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(DateOnly)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(DateOnly)),
                                convertDateOnly,
                                Expression.Constant("O"),
                                Expression.Constant(CultureInfo.InvariantCulture)))),
                    Expression.IfThen(
                        Expression.TypeIs(intermediate, typeof(TimeOnly)),
                        Expression.Return(
                            result,
                            Expression.Call(
                                Expression.Convert(intermediate, typeof(TimeOnly)),
                                convertTimeOnly,
                                Expression.Constant("O"),
                                Expression.Constant(CultureInfo.InvariantCulture)))),
#endif
                    Expression.IfThen(
                        Expression.Property(Expression.Call(intermediate, getType), isEnum),
                        Expression.Return(
                            result,
                            Expression.Call(
                                null,
                                convertEnum,
                                Expression.Call(intermediate, getType),
                                intermediate))),
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
#if NET6_0_OR_GREATER
                else if (value.Type == typeof(DateOnly))
                {
                    var convertDateOnly = typeof(DateOnly)
                        .GetMethod(nameof(DateOnly.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                    value = Expression.Call(
                        value,
                        convertDateOnly,
                        Expression.Constant("O"),
                        Expression.Constant(CultureInfo.InvariantCulture));
                }
                else if (value.Type == typeof(TimeOnly))
                {
                    var convertTimeOnly = typeof(TimeOnly)
                        .GetMethod(nameof(TimeOnly.ToString), new[] { typeof(string), typeof(IFormatProvider) });

                    value = Expression.Call(
                        value,
                        convertTimeOnly,
                        Expression.Constant("O"),
                        Expression.Constant(CultureInfo.InvariantCulture));
                }
#endif
                else if (value.Type.IsEnum)
                {
                    var cases = value.Type.GetEnumMembers()
                        .Select(field => Expression.SwitchCase(
                            Expression.Constant(field.GetEnumMemberName()),
                            Expression.Constant(Enum.Parse(value.Type, field.Name))));

                    var exceptionConstructor = typeof(ArgumentException)
                        .GetConstructor(new[] { typeof(string) });

                    value = Expression.Switch(
                        value,
                        Expression.Throw(
                            Expression.New(
                                exceptionConstructor,
                                Expression.Constant($"Value does not correspond to a named enum member.")),
                            target),
                        cases.ToArray());
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
