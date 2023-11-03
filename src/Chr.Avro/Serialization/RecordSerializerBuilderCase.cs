namespace Chr.Avro.Serialization
{
    using System;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Runtime.Serialization;
    using System.Text.RegularExpressions;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;
    using Microsoft.CSharp.RuntimeBinder;

    using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="RecordSchema" />.
    /// </summary>
    public abstract class RecordSerializerBuilderCase : SerializerBuilderCase
    {
        private static readonly Regex FuzzyCharacters = new(@"[^A-Za-z0-9]");

        /// <summary>
        /// Builds an <see cref="Expression" /> representing a dynamic get of a field or
        /// property.
        /// </summary>
        /// <param name="object">
        /// An <see cref="Expression" /> representing an object of an unknown type.
        /// </param>
        /// <param name="name">
        /// The name of the member to get.
        /// </param>
        /// <returns>
        /// An <see cref="Expression" /> representing a dynamic get of <paramref name="name" />
        /// on <paramref name="object" />.
        /// </returns>
        protected virtual Expression BuildDynamicGet(Expression @object, string name)
        {
            var getType = typeof(object)
                .GetMethod(nameof(object.GetType), Type.EmptyTypes);

            var getMembers = typeof(Type)
                .GetMethod(nameof(Type.GetMember), new[] { typeof(string) });

            var getValue = typeof(ReflectionExtensions)
                .GetMethod(nameof(ReflectionExtensions.GetValue), new[] { typeof(MemberInfo), typeof(object) });

            var flags = CSharpBinderFlags.None;
            var infos = new[] { CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null) };
            var binder = Binder.GetMember(flags, name, @object.Type, infos);

            var type = Expression.Parameter(typeof(Type));
            var members = Expression.Parameter(typeof(MemberInfo[]));

            return Expression.Block(
                new[] { members, type },
                Expression.Assign(type, Expression.Call(@object, getType)),
                Expression.Assign(members, Expression.Call(type, getMembers, Expression.Constant(name))),
                Expression.Condition(
                    Expression.Equal(Expression.ArrayLength(members), Expression.Constant(0)),
                    Expression.Dynamic(binder, typeof(object), @object),
                    Expression.Call(null, getValue, Expression.ArrayAccess(members, Expression.Constant(0)), @object)));
        }

        /// <summary>
        /// Determines whether a <see cref="RecordField" /> is a match for a type member.
        /// </summary>
        /// <param name="field">
        /// A record field.
        /// </param>
        /// <param name="member">
        /// The member to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="member" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(RecordField field, MemberInfo member)
        {
            if (member.DeclaringType.HasAttribute<DataContractAttribute>())
            {
                if (member.GetAttribute<DataMemberAttribute>() is DataMemberAttribute memberAttribute)
                {
                    if (string.IsNullOrEmpty(memberAttribute.Name))
                    {
                        return IsMatch(field, member.Name);
                    }
                    else
                    {
                        return field.Name == memberAttribute.Name;
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
                    && IsMatch(field, member.Name);
            }
        }

        /// <summary>
        /// Determines whether a <see cref="RecordField" /> name matches another name.
        /// </summary>
        /// <param name="field">
        /// A record field.
        /// </param>
        /// <param name="name">
        /// The name to compare.
        /// </param>
        /// <returns>
        /// <c>true</c> if <paramref name="name" /> is a match; <c>false</c> otherwise.
        /// </returns>
        protected virtual bool IsMatch(RecordField field, string name)
        {
            return string.Equals(
                FuzzyCharacters.Replace(field.Name, string.Empty),
                FuzzyCharacters.Replace(name, string.Empty),
                StringComparison.InvariantCultureIgnoreCase);
        }
    }
}
