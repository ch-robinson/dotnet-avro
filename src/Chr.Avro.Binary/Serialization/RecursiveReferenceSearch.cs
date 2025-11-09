namespace Chr.Avro.Serialization;

using System.Collections.Generic;
using System.Diagnostics;
using Chr.Avro.Abstract;

/// <summary>
/// Utility class that traverses a given <see cref="Schema" /> and identifies which elements of the schema
/// are on potentially recursive groups
/// </summary>
internal static class RecursiveReferenceSearch
{
    /// <summary>
    /// Traverses <paramref name="schema"/> and identifies recursive groups
    /// </summary>
    /// <param name="schema">Schema to traverse.</param>
    /// <param name="results">A dictionary into which the results are written.</param>
    public static void Collect(Schema schema, IDictionary<Schema, bool> results)
    {
        var context = new Context(results);
        Collect(schema, context);
    }

    private static void Collect(Schema schema, Context context)
    {
        if (context.Results.ContainsKey(schema))
        {
            // We have already visited this schema, there's no need to visit again
            return;
        }

        if (!context.VisitedOnCurrentPath.Add(schema))
        {
            // We are re-visiting a type that is already on the current path, i.e. we have a recursion.
            // We will add every type that we have visited on the current path since last time we encountered schema to the results.
            for (var i = context.CurrentPath.Count - 1; i >= 0; i--)
            {
                var item = context.CurrentPath[i];

                context.Results[item] = true;

                if (Equals(item, schema))
                {
                    break;
                }
            }

            return;
        }

        context.CurrentPath.Add(schema);

        switch (schema)
        {
            case RecordSchema recordSchema:
                Collect(recordSchema, context);
                break;
            case UnionSchema unionSchema:
                Collect(unionSchema, context);
                break;
            case ArraySchema arraySchema:
                Collect(arraySchema, context);
                break;
            case MapSchema mapSchema:
                Collect(mapSchema, context);
                break;
        }

        if (!context.Results.TryGetValue(schema, out var isRecursive) || !isRecursive)
        {
            context.Results[schema] = false;
        }

        Debug.Assert(context.VisitedOnCurrentPath.Contains(schema), "Schema not found on current path anymore");
        Debug.Assert(context.CurrentPath.Count > 0 && Equals(context.CurrentPath[context.CurrentPath.Count - 1], schema), "List was not cleaned up properly");

        context.CurrentPath.RemoveAt(context.CurrentPath.Count - 1);
        context.VisitedOnCurrentPath.Remove(schema);
    }

    private static void Collect(RecordSchema schema, Context context)
    {
        foreach (var field in schema.Fields)
        {
            Collect(field.Type, context);
        }
    }

    private static void Collect(UnionSchema schema, Context context)
    {
        foreach (var innerSchema in schema.Schemas)
        {
            Collect(innerSchema, context);
        }
    }

    private static void Collect(ArraySchema schema, Context context)
    {
        Collect(schema.Item, context);
    }

    private static void Collect(MapSchema schema, Context context)
    {
        Collect(schema.Value, context);
    }

    private sealed class Context
    {
        public Context(IDictionary<Schema, bool> results)
        {
            Results = results;
        }

        public IDictionary<Schema, bool> Results { get; }

        public HashSet<Schema> VisitedOnCurrentPath { get; } = new HashSet<Schema>();

        public List<Schema> CurrentPath { get; } = new List<Schema>();
    }
}
