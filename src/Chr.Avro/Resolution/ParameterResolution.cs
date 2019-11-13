using System;
using System.Reflection;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// Contains resolved information about a parameter
    /// </summary>
    public class ParameterResolution
    {
        private ParameterInfo parameter;

        private IdentifierResolution name;

        private Type type;

        /// <summary>
        /// The resolved parameter reflection info.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the reflection info is set to null.
        /// </exception>
        public virtual ParameterInfo Parameter
        {
            get
            {
                return parameter ?? throw new InvalidOperationException();
            }
            set
            {
                parameter = value ?? throw new ArgumentNullException(nameof(value), "Parameter reflection info cannot be null.");
            }
        }

        /// <summary>
        /// The parameter name.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the reflection info is set to null.
        /// </exception>
        public virtual IdentifierResolution Name
        {
            get
            {
                return name ?? throw new InvalidOperationException();
            }
            set
            {
                name = value ?? throw new ArgumentNullException(nameof(value), "Parameter name cannot be null.");
            }
        }

        /// <summary>
        /// The parameter type.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the reflection info is set to null.
        /// </exception>
        public virtual Type Type
        {
            get
            {
                return type ?? throw new InvalidOperationException();
            }
            set
            {
                type = value ?? throw new ArgumentNullException(nameof(value), "Parameter type cannot be null.");
            }
        }

        /// <summary>
        /// Creates a new parameter resolution.
        /// </summary>
        /// <param name="parameter">
        /// The resolved parameter reflection info.
        /// </param>
        /// <param name="type">
        /// The parameter type.
        /// </param>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// Thrown when reflection info, type or name is null.
        /// </exception>
        public ParameterResolution(ParameterInfo parameter, Type type, IdentifierResolution name)
        {
            Parameter = parameter;
            Name = name;
            Type = type;
        }
    }
}
