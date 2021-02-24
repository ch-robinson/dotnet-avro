namespace Chr.Avro.Resolution
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Represents the outcome of a <see cref="ITypeResolverCase{TypeResolverCaseResult}" />.
    /// </summary>
    public class TypeResolverCaseResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TypeResolverCaseResult" /> class.
        /// </summary>
        public TypeResolverCaseResult()
        {
            Exceptions = new List<Exception>();
        }

        /// <summary>
        /// Gets or sets exceptions regarding the inapplicability of the case. If <see cref="TypeResolution" />
        /// is not <c>null</c>, these exceptions should be interpreted as warnings.
        /// </summary>
        public virtual ICollection<Exception> Exceptions { get; set; }

        /// <summary>
        /// Gets or sets the type resolution obtained by applying the case. If <c>null</c>, the
        /// case was not applied successfully.
        /// </summary>
        public virtual TypeResolution? TypeResolution { get; set; }

        /// <summary>
        /// Creates a new <see cref="TypeResolverCaseResult" /> for an unsuccessful outcome.
        /// </summary>
        /// <param name="exception">
        /// An exception describing the inapplicability of the case.
        /// </param>
        /// <returns>
        /// A <see cref="TypeResolverCaseResult" /> with <see cref="Exceptions" /> populated and
        /// <see cref="TypeResolution" /> <c>null</c>.
        /// </returns>
        public static TypeResolverCaseResult FromException(Exception exception)
        {
            var result = new TypeResolverCaseResult();
            result.Exceptions.Add(exception);

            return result;
        }

        /// <summary>
        /// Creates a new <see cref="TypeResolverCaseResult" /> for an unsuccessful outcome.
        /// </summary>
        /// <param name="typeResolution">
        /// The type resolution obtained by applying the case.
        /// </param>
        /// <returns>
        /// A <see cref="TypeResolverCaseResult" /> with <see cref="TypeResolution" /> populated.
        /// </returns>
        public static TypeResolverCaseResult FromTypeResolution(TypeResolution typeResolution)
        {
            return new TypeResolverCaseResult
            {
                TypeResolution = typeResolution,
            };
        }
    }
}
