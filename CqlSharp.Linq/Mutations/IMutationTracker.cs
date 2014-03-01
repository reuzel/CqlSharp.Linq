using System.Collections.Generic;

namespace CqlSharp.Linq.Mutations
{
    internal interface IMutationTracker
    {
        /// <summary>
        /// Gets the changed objects.
        /// </summary>
        /// <returns></returns>
        IEnumerable<ITrackedObject> GetChangedObjects();
    }
}