namespace Chr.Avro.Infrastructure
{
    using System;
    using System.Dynamic;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Provides binder implementations for dynamic operations.
    /// </summary>
    internal static class Binders
    {
        /// <summary>
        /// Creates a new <see cref="GetMemberBinder" /> that falls back to an optional default value.
        /// </summary>
        /// <param name="name">
        /// The name of the member to get (case sensitive).
        /// </param>
        /// <param name="defaultValue">
        /// An optional value to return when no member matching <paramref name="name" /> is
        /// present. If null, get will throw <see cref="MissingMemberException" /> if the member
        /// does not exist.
        /// </param>
        /// <returns>
        /// A <see cref="GetMemberBinder" /> instance.
        /// </returns>
        public static GetMemberBinder GetMember(
            string name,
            DefaultValue? defaultValue = default)
        {
            var fallback = defaultValue switch
            {
                null => null,
                _ => Expression.Constant(defaultValue.ToObject<dynamic>(), typeof(object)),
            };

            return new DynamicGetMemberBinder(name, fallback);
        }

        private sealed class DynamicGetMemberBinder : GetMemberBinder
        {
            private static readonly ConstructorInfo MissingMemberExceptionConstructor =
                typeof(MissingMemberException)
                    .GetConstructor(new[] { typeof(string), typeof(string) })!;

            public DynamicGetMemberBinder(string name, Expression? fallback)
                : base(name, ignoreCase: false)
            {
                Fallback = fallback;
            }

            public Expression? Fallback { get; }

            public override DynamicMetaObject FallbackGetMember(
                DynamicMetaObject target,
                DynamicMetaObject? errorSuggestion)
            {
                if (errorSuggestion is not null)
                {
                    return errorSuggestion;
                }

                // eventually allow configurable binding flags/matching logic here?
                var match = target.RuntimeType.GetMember(Name)
                    .Where(m => m.MemberType is MemberTypes.Field or MemberTypes.Property)
                    .SingleOrDefault();

                return new DynamicMetaObject(
                    match switch
                    {
                        FieldInfo field => Expression.TypeAs(
                            Expression.Field(Expression.Constant(target.Value), field),
                            ReturnType),
                        PropertyInfo property => Expression.TypeAs(
                            Expression.Property(Expression.Constant(target.Value), property),
                            ReturnType),
                        _ => Fallback ?? Expression.Throw(
                            Expression.New(
                                MissingMemberExceptionConstructor,
                                Expression.Constant(target.RuntimeType.Name),
                                Expression.Constant(Name)),
                            ReturnType),
                    },
                    BindingRestrictions.Empty);
            }
        }
    }
}
