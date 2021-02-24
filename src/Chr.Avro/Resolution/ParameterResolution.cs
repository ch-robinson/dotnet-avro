namespace Chr.Avro.Resolution
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Represents resolved information about a constructor or method parameter.
    /// </summary>
    public class ParameterResolution
    {
        private ParameterInfo parameter = default!;

        private IdentifierResolution name = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParameterResolution" /> class.
        /// </summary>
        /// <param name="parameter">
        /// The resolved <see cref="ParameterInfo" />.
        /// </param>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        public ParameterResolution(ParameterInfo parameter, IdentifierResolution name)
        {
            Parameter = parameter;
            Name = name;
        }

        /// <summary>
        /// Gets or sets the resolved <see cref="ParameterInfo" />.
        /// </summary>
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
        /// Gets or sets the parameter name.
        /// </summary>
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
    }
}
