using Chr.Avro.Abstract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Chr.Avro.Resolution
{
    /// <summary>
    /// Contains resolved information about a constructor.
    /// </summary>
    public class ConstructorResolution
    {
        private ConstructorInfo constructor;

        /// <summary>
        /// The resolved constructor reflection info.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// Thrown when the constructor info is set to null.
        /// </exception>
        public virtual ConstructorInfo Constructor
        {
            get
            {
                return constructor ?? throw new InvalidOperationException();
            }
            set
            {
                constructor = value ?? throw new ArgumentNullException(nameof(value), "Constructor reflection info cannot be null.");
            }
        }

        /// <summary>
        /// The constructors parameters.
        /// </summary>
        public virtual ICollection<ParameterResolution> Parameters { get; set; }

        /// <summary>
        /// Creates a new constructor resolution.
        /// </summary>
        /// <param name="constructor">
        /// The resolved constructor reflection info.
        /// </param>
        /// <param name="parameters">
        /// The constructors parameters.
        /// </param>
        public ConstructorResolution(ConstructorInfo constructor, ICollection<ParameterResolution> parameters = null)
        {
            Constructor = constructor;
            Parameters = parameters;
        }
    }
}
