namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// State of a tracked object
    /// </summary>
    internal enum ObjectState
    {
        Unchanged,
        Added,
        Modified,
        Deleted
    }
}