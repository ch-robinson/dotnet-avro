using System;
using System.IO;
using System.Reflection;

namespace Chr.Avro.Cli
{
    public interface IClrTypeOptions
    {
        string AssemblyName { get; }

        string TypeName { get; }
    }

    internal static class TypeOptionExtensions
    {
        public static Assembly ResolveAssembly(this IClrTypeOptions options)
        {
            if (string.IsNullOrEmpty(options.AssemblyName))
            {
                return null;
            }

            try
            {
                return Assembly.Load(options.AssemblyName);
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
                return Assembly.LoadFrom(Path.GetFullPath(options.AssemblyName));
            }
            catch (FileNotFoundException)
            {
                throw new ProgramException(message: "The assembly could not be found. Make sure that you’ve provided either a recognizable name (e.g. System.Runtime) or a valid assembly path.");
            }
            catch (BadImageFormatException)
            {
                throw new ProgramException(message: "The assembly is not valid. Check that the path you’re providing points to a valid assembly file.");
            }
        }

        public static Type ResolveType(this IClrTypeOptions options)
        {
            if (options.ResolveAssembly() is var assembly && assembly == null)
            {
                try
                {
                    return Type.GetType(options.TypeName, ignoreCase: true, throwOnError: true);
                }
                catch (TypeLoadException)
                {
                    throw new ProgramException(message: "The type could not be found. You may need to provide an assembly as well.");
                }
            }
            else
            {
                try
                {
                    return assembly.GetType(options.TypeName, ignoreCase: true, throwOnError: true);
                }
                catch (TypeLoadException)
                {
                    throw new ProgramException(message: "The type could not be found in the provided assembly.");
                }
            }
        }
    }
}
