using System.Collections.Generic;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// Non-generic interface toward table change trackers
    /// </summary>
    internal interface ITableMutationTracker
    {
        /// <summary>
        /// Gets the tracked table entries.
        /// </summary>
        /// <returns></returns>
        IEnumerable<TrackedObject> Entries();
    }
}