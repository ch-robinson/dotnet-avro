using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace Chr.Avro.Cli
{
    public interface IClrTypeOptions
    {
        IEnumerable<string> AssemblyNames { get; }

        string TypeName { get; }
    }

    internal static class TypeOptionExtensions
    {
        public static Type ResolveType(this IClrTypeOptions options)
        {
            var assemblies = options.AssemblyNames
                .Select(name =>
                {
                    try
                    {
                        return Assembly.Load(name);
                    }
                    catch (FileNotFoundException)
                    {
                        // nbd
                    }
                    catch (FileLoadException)
                    {
                        // also nbd
                    }

                    try
                    {
                        return Assembly.LoadFrom(Path.GetFullPath(name));
                    }
                    catch (FileNotFoundException)
                    {
                        throw new ProgramException(message: $"{name} could not be found. Make sure that you’ve provided either a recognizable name (e.g. System.Runtime) or a valid assembly path.");
                    }
                    catch (BadImageFormatException)
                    {
                        throw new ProgramException(message: $"{name} is not valid. Check that the path you’re providing points to a valid assembly file.");
                    }
                })
                .ToDictionary(type => type.GetName());

            try
            {
                return Type.GetType(
                    options.TypeName,
                    assemblyResolver: name => assemblies.GetValueOrDefault(name),
                    typeResolver: (assembly, name, ignoreCase) =>
                    {
                        if (assembly == null)
                        {
                            foreach (var candidate in assemblies.Values)
                            {
                                if (candidate.GetType(name, ignoreCase: ignoreCase, throwOnError: false) is Type result)
                                {
                                    return result;
                                }
                            }

                            return null;
                        }

                        return assembly.GetType(name, ignoreCase: ignoreCase, throwOnError: false);
                    },
                    ignoreCase: true,
                    throwOnError: true
                );
            }
            catch (TypeLoadException)
            {
                throw new ProgramException(message: "The type could not be found. You may need to provide additional assemblies.");
            }
        }
    }
}
