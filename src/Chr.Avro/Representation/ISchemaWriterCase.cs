namespace Chr.Avro.Representation
{
    /// <summary>
    /// Defines methods to write specific Avro schemas.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as the write operation progresses.
    /// </typeparam>
    /// <typeparam name="TResult">
    /// The type of object used to represent the case result.
    /// </typeparam>
    public interface ISchemaWriterCase<TContext, TResult>
    {
    }
}
