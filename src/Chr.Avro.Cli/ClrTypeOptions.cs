using System;
using System.Collections.Generic;
using System.IO;
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
            foreach (var assembly in options.AssemblyNames)
            {
                try
                {
                    Assembly.Load(assembly);
                    continue;
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
                    Assembly.LoadFrom(Path.GetFullPath(assembly));
                }
                catch (FileNotFoundException)
                {
                    throw new ProgramException(message: $"{assembly} could not be found. Make sure that you’ve provided either a recognizable name (e.g. System.Runtime) or a valid assembly path.");
                }
                catch (BadImageFormatException)
                {
                    throw new ProgramException(message: $"{assembly} is not valid. Check that the path you’re providing points to a valid assembly file.");
                }
            }

            try
            {
                return Type.GetType(options.TypeName, ignoreCase: true, throwOnError: true);
            }
            catch (TypeLoadException)
            {
                throw new ProgramException(message: "The type could not be found. You may need to provide additional assemblies.");
            }
        }
    }
}
