namespace Chr.Avro.Serialization
{
    using System;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="EnumSchema" />.
    /// </summary>
    public abstract class EnumSerializerBuilderCase : SerializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

        /// <remarks>
        /// This override includes additional conditions to handle conversions from enumerators to
        /// strings when the enum type is not known in advance. If <paramref name="value" /> is not
        /// an enumerator, the base implementation is used.
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
        /// Determines whether an enum symbol is a match for a type member.
        /// </summary>
        /// <param name="symbol">
        /// An enum symbol.
        /// </param>
        /// <param name="member">
        /// The member to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="member" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(string symbol, MemberInfo member)
        {
            if (member.DeclaringType.HasAttribute<DataContractAttribute>())
            {
                if (member.GetAttribute<EnumMemberAttribute>() is EnumMemberAttribute memberAttribute)
                {
                    if (string.IsNullOrEmpty(memberAttribute.Value))
                    {
                        return IsMatch(symbol, member.Name);
                    }
                    else
                    {
                        return symbol == memberAttribute.Value;
                    }
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return !(member.HasAttribute<IgnoreDataMemberAttribute>() || member.HasAttribute<NonSerializedAttribute>())
                    && IsMatch(symbol, member.Name);
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
