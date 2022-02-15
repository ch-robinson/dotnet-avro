namespace Chr.Avro.Codegen
{
    using System.IO;
    using Chr.Avro.Abstract;

    /// <summary>
    /// Generates code to match Avro schemas.
    /// </summary>
    public interface ICodeGenerator
    {
        /// <summary>
        /// Writes a compilation unit (intuitively, a single source code file) that contains types
        /// that match the schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate code for.
        /// </param>
        /// <param name="stream">
        /// A stream to write the resulting compilation unit to.
        /// </param>
        void WriteCompilationUnit(Schema schema, Stream stream);

        /// <summary>
        /// Writes a compilation unit (intuitively, a single source code file) that contains types
        /// that match the schema.
        /// </summary>
        /// <param name="schema">
        /// The schema to generate code for.
        /// </param>
        /// <returns>
        /// The compilation unit as a string.
        /// </returns>
        string WriteCompilationUnit(Schema schema);
    }
}
