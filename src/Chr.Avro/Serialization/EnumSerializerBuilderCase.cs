namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="EnumSchema" />.
    /// </summary>
    public abstract class EnumSerializerBuilderCase : SerializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

        /// <remarks>
        /// This override includes additional conditions to handle conversions to types that can be
        /// idiomatically represented as strings. If none match, the base implementation is used.
        /// </remarks>
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target == typeof(string))
            {
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
                        Expression.Property(Expression.Call(intermediate, getType), isEnum),
                        Expression.Return(result, Expression.Call(intermediate, toString))),
                    Expression.Label(result, base.BuildDynamicConversion(intermediate, target)));
            }
            else
            {
                return base.BuildDynamicConversion(value, target);
            }
        }

        /// <summary>
        /// Determines whether an enum symbol matches another name.
        /// </summary>
        /// <param name="symbol">
        /// An enum symbol.
        /// </param>
        /// <param name="name">
        /// The name to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="name" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(string symbol, string name)
        {
            return string.Equals(
                FuzzyCharacters.Replace(symbol, string.Empty),
                FuzzyCharacters.Replace(name, string.Empty),
                StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
