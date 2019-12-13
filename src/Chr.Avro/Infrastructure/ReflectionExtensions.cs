using System;
using System.Collections.Generic;
using System.Linq;

namespace Chr.Avro.Infrastructure
{
    internal static class ReflectionExtensions
    {
        public static Type? GetEnumerableType(this Type type)
        {
            return new[] { type }
                .Concat(type.GetInterfaces())
                .SingleOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))?
                .GetGenericArguments()?
                .ElementAt(0);
        }
    }
}
