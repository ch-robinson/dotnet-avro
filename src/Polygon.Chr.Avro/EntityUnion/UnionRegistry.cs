using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ApplicationException = System.ApplicationException;

namespace Polygon.Chr.Avro.EntityUnion
{
    public class UnionRegistry
    {
        public Dictionary<Type, List<Type>> Types { get; set; } = new Dictionary<Type, List<Type>>();

        public UnionRegistry() { }

        public void RegisterImplementationsOf(Type interfaceType, Assembly? assembly = null)
        {
            assembly = assembly ?? interfaceType.Assembly;
            Type[] types;
            try
            {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException e)
            {
                types = e.Types.Where(z => z != null).ToArray();
            }

            var l = new List<Type>();
            foreach (var t in types)
            {
                if (!t.IsInterface && t.GetInterfaces().Contains(interfaceType))
                    l.Add(t);
            }

            if (l.Count < 2)
                throw new ApplicationException($"Union schema invalid for {interfaceType.Name}");
            Types[interfaceType] = l;


        }
    }
}
