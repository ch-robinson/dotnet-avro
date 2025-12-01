namespace Chr.Avro.Serialization
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Chr.Avro.Abstract;
    using Chr.Avro.Infrastructure;
    using Microsoft.CSharp.RuntimeBinder;
    using Binder = Microsoft.CSharp.RuntimeBinder.Binder;

    /// <summary>
    /// Provides a base implementation for serializer builder cases that match <see cref="ArraySchema" />.
    /// </summary>
    public abstract class ArraySerializerBuilderCase : SerializerBuilderCase
    {
        /// <inheritdoc />
        protected override Expression BuildDynamicConversion(Expression value, Type target)
        {
            if (target.GetEnumerableType() is Type itemType)
            {
                var collectionType = typeof(IReadOnlyCollection<>).MakeGenericType(itemType);
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);

                var toList = typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.ToList))
                    .MakeGenericMethod(itemType);

                return Expression.Convert(
                    Expression.Call(
                        null,
                        toList,
                        Expression.Convert(
                            Expression.Dynamic(
                                Binder.InvokeMember(
                                    CSharpBinderFlags.None,
                                    nameof(Cast),
                                    null,
                                    typeof(ArraySerializerBuilderCase),
                                    new[]
                                    {
                                        CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.IsStaticType, null),
                                        CSharpArgumentInfo.Create(CSharpArgumentInfoFlags.None, null),
                                    }),
                                typeof(object),
                                Expression.Constant(typeof(ArraySerializerBuilderCase)),
                                value),
                            enumerableType)),
                    collectionType);
            }
            else
            {
                return base.BuildDynamicConversion(value, target);
            }
        }

        /// <inheritdoc />
        protected override Expression BuildStaticConversion(Expression value, Type target)
        {
            if (target.GetEnumerableType() is Type itemType)
            {
                var collectionType = typeof(IReadOnlyCollection<>).MakeGenericType(itemType);
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(itemType);

                if (!collectionType.IsAssignableFrom(value.Type))
                {
                    var toList = typeof(Enumerable)
                        .GetMethod(nameof(Enumerable.ToList))
                        .MakeGenericMethod(itemType);

                    value = Expression.Call(
                        null,
                        toList,
                        base.BuildStaticConversion(value, enumerableType));
                }

                return base.BuildStaticConversion(value, collectionType);
            }
            else
            {
                return base.BuildStaticConversion(value, target);
            }
        }

        /// <summary>
        /// Gets the item <see cref="Type" /> of an enumerable <see cref="Type" />.
        /// </summary>
        /// <param name="type">
        /// A <see cref="Type" /> object that describes a generic enumerable.
        /// </param>
        /// <returns>
        /// If <paramref name="type" /> implements (or is) <see cref="IEnumerable{T}" />, its type
        /// argument; <c>null</c> otherwise.
        /// </returns>
        protected virtual Type? GetEnumerableType(Type type)
        {
            return type.GetEnumerableType();
        }

        /// <summary>
        /// Creates an enumerable that ensures items from the source enumerable are boxed.
        /// </summary>
        /// <param name="enumerable">
        /// An enumerable of any type.
        /// </param>
        /// <returns>
        /// An enumerable whose items are guaranteed to be boxed.
        /// </returns>
        private static IEnumerable<object?> Cast<T>(IEnumerable<T> enumerable)
        {
            foreach (object? item in enumerable)
            {
                yield return item;
            }
        }

        /// <summary>
        /// A helper class meant to facilitate the runtime generation of code that iterates over collections.
        /// It resolves and caches the necessary MethodInfo and PropertyInfo metadata required to build
        /// a "foreach" loop using Expression Trees.
        /// </summary>
        internal class EnumerationReflection
        {
            private EnumerationReflection(
                ParameterExpression enumerator,
                PropertyInfo getCount,
                PropertyInfo getCurrent,
                MethodInfo getEnumerator,
                MethodInfo moveNext,
                MethodCallExpression? disposeCall)
            {
                Enumerator = enumerator;
                GetCount = getCount;
                GetCurrent = getCurrent;
                GetEnumerator = getEnumerator;
                MoveNext = moveNext;
                DisposeCall = disposeCall;
            }

            /// <summary>
            /// Gets the expression variable that holds the Enumerator instance (e.g., the 'var enum' in a foreach).
            /// </summary>
            public ParameterExpression Enumerator { get; }

            /// <summary>
            /// Gets the <see cref="PropertyInfo"/> for the collection's Count or Length property.
            /// </summary>
            public PropertyInfo GetCount { get; }

            /// <summary>
            /// Gets the <see cref="PropertyInfo"/> for the <c>Current</c> property of the enumerator.
            /// </summary>
            public PropertyInfo GetCurrent { get; }

            /// <summary>
            /// Gets the <see cref="MethodInfo"/> for the <c>GetEnumerator()</c> method.
            /// </summary>
            public MethodInfo GetEnumerator { get; }

            /// <summary>
            /// Gets the <see cref="MethodInfo"/> for the <c>MoveNext()</c> method.
            /// </summary>
            public MethodInfo MoveNext { get; }

            /// <summary>
            /// Gets the <see cref="MethodCallExpression"/> representing the <c>Dispose()</c> call.
            /// This may be null if the enumerator does not implement <see cref="IDisposable"/>
            /// (common in some struct enumerators for performance).
            /// </summary>
            public MethodCallExpression? DisposeCall { get; }

            /// <summary>
            /// Analyzes the provided collection type and resolves the reflection metadata needed to iterate it.
            /// </summary>
            /// <param name="collection">The expression representing the collection instance.</param>
            /// <param name="readOnlyCollectionType">The fallback type for read-only collections (usually <see cref="IReadOnlyCollection{T}"/>).</param>
            /// <param name="enumerableType">The fallback type for enumerables (usually <see cref="IEnumerable{T}"/> or <see cref="IEnumerable"/>).</param>
            /// <returns>A populated <see cref="EnumerationReflection"/> instance.</returns>
            public static EnumerationReflection Create(ParameterExpression collection, Type readOnlyCollectionType, Type enumerableType)
            {
                // Try to get the Count property from the actual concrete type first.
                // If not found (e.g., explicit interface impl), fallback to looking up ICollection or IReadOnlyCollection.
                var getCount = GetCountProperty(collection.Type, readOnlyCollectionType);

                // Similarly, look on the concrete type (to avoid boxing struct enumerators like List<T>.Enumerator),
                // falling back to the generic IEnumerable<T> interface if needed.
                var getEnumerator = GetMethodWithFallback(collection.Type, nameof(IEnumerable.GetEnumerator), enumerableType)!;

                // CRITICAL: IEnumerator<T> inherits from IEnumerator (non-generic).
                // The MoveNext method is defined on the non-generic base interface.
                // If the return type of GetEnumerator is strictly IEnumerator<T>, standard reflection might not
                // see MoveNext immediately without checking the interface hierarchy.
                var moveNext = GetMethodWithFallback(getEnumerator.ReturnType, nameof(IEnumerator.MoveNext), typeof(IEnumerator))!;

                // Creates a variable expression that will hold the result of calling getEnumerator
                var enumerator = Expression.Variable(getEnumerator.ReturnType);

                // Look for the 'Current' property on the enumerator type returned from the getEnumerator resolved earlier.
                var getCurrent = enumerator.Type.GetProperty(nameof(IEnumerator.Current))!;

                // Check if the specific enumerator type implements IDisposable.
                var dispose = GetMethodWithFallback(enumerator.Type, nameof(IDisposable.Dispose), typeof(IDisposable));
                var disposeCall = default(MethodCallExpression);
                if (dispose is not null)
                {
                    // Some Enumerator implementations (like ImmutableArray<T>.Enumerator and internal ArrayEnumerator)
                    // do NOT implement IDisposable.
                    // By checking for null here, the generated code can skip the overhead of a try/finally block
                    // if it is not strictly necessary.
                    disposeCall = Expression.Call(enumerator, dispose);
                }

                return new EnumerationReflection(enumerator, getCount, getCurrent, getEnumerator, moveNext, disposeCall);
            }

            private static PropertyInfo GetCountProperty(Type type, Type fallback)
            {
                // Try to get the property from the concrete type (e.g., List<int>)
                var property = type.GetProperty(nameof(ICollection.Count));
                if (property is not null)
                {
                    return property;
                }

                // If not found, get from the fallback type
                Debug.Assert(fallback.IsAssignableFrom(type), "Fallback should aways be IReadOnlyCollection and BuildConversion will result in an IReadOnlyCollection");
                property = fallback.GetProperty(nameof(ICollection.Count))!;
                return property;
            }

            private static MethodInfo? GetMethodWithFallback(Type type, string name, Type fallback)
            {
                // Try to get the method from the concrete type (e.g., List<int>)
                var method = type.GetMethod(name);
                if (method is not null)
                {
                    return method;
                }

                // If not found, and the type implements the fallback interface (IReadOnlyCollection, IEnumerable, IDisposable),
                // get the method from that interface.
                if (fallback.IsAssignableFrom(type))
                {
                    method = fallback.GetMethod(name);
                }

                return method;
            }
        }
    }
}
