namespace Chr.Avro.Representation
{
    /// <summary>
    /// Defines methods to read specific Avro schemas.
    /// </summary>
    /// <typeparam name="TContext">
    /// The type of object used to accumulate results as the read operation progresses.
    /// </typeparam>
    /// <typeparam name="TResult">
    /// The type of object used to represent the case result.
    /// </typeparam>
    public interface ISchemaReaderCase<TContext, TResult>
    {
    }
}
