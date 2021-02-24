namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Represents resolved information about a constructor.
    /// </summary>
    public class ConstructorResolution
    {
        private ConstructorInfo constructor = default!;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConstructorResolution" /> class.
        /// </summary>
        /// <param name="constructor">
        /// The resolved <see cref="ConstructorInfo" />.
        /// </param>
        /// <param name="parameters">
        /// The resolved <see cref="ParameterResolution" />s.
        /// </param>
        public ConstructorResolution(ConstructorInfo constructor, IEnumerable<ParameterResolution>? parameters = null)
        {
            Constructor = constructor;
            Parameters = parameters?.ToList() ?? new List<ParameterResolution>();
        }

        /// <summary>
        /// Gets or sets the resolved <see cref="ConstructorInfo" />.
        /// </summary>
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
        /// Gets or sets the resolved <see cref="ParameterResolution" />s.
        /// </summary>
        public virtual ICollection<ParameterResolution> Parameters { get; set; }
    }
}
