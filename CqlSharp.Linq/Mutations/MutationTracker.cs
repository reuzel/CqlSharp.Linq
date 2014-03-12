// CqlSharp.Linq - CqlSharp.Linq
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Tracks the mutation accross all tables
    /// </summary>
    public class MutationTracker
    {
        /// <summary>
        /// The table mutation trackers
        /// </summary>
        private readonly ConcurrentDictionary<ICqlTable, ITableMutationTracker> _tableTrackers;

        /// <summary>
        /// Initializes a new instance of the <see cref="MutationTracker"/> class.
        /// </summary>
        internal MutationTracker()
        {
            _tableTrackers = new ConcurrentDictionary<ICqlTable, ITableMutationTracker>();
        }

        /// <summary>
        /// Gets the tracker for the given table.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        internal TableMutationTracker<TEntity> GetTracker<TEntity>(CqlTable<TEntity> table) where TEntity : class, new()
        {
            return (TableMutationTracker<TEntity>)_tableTrackers.GetOrAdd(table, (t) => new TableMutationTracker<TEntity>((CqlTable<TEntity>)t));
        }

        /// <summary>
        /// Returns the tracked entries
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TrackedObject> Entries()
        {
            return _tableTrackers.Values.SelectMany(tt => tt.Entries());
        }

        /// <summary>
        /// Returns the tracked entries of a specific type
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <returns></returns>
        public IEnumerable<TrackedObject<TEntity>> Entries<TEntity>() where TEntity : class, new()
        {
            ITableMutationTracker mutationTracker = _tableTrackers
                .Where(kvp => kvp.Key.Type == typeof(TEntity))
                .Select(kvp => kvp.Value)
                .FirstOrDefault();

            if (mutationTracker != null)
                return mutationTracker.Entries().Cast<TrackedObject<TEntity>>();

            return Enumerable.Empty<TrackedObject<TEntity>>();
        }

        /// <summary>
        ///   Detects any changes made to tracked objects
        /// </summary>
        /// <returns> true if any object contains a change to be committed to the server </returns>
        public bool DetectChanges()
        {
            bool hasChanges = false;
            foreach (var trackedObject in Entries())
            {
                hasChanges |= trackedObject.DetectChanges();
            }
            return hasChanges;
        }

        /// <summary>
        ///   Detects any changes made to tracked objects
        /// </summary>
        /// <returns> true if any object contains a change to be committed to the server </returns>
        public bool HasChanges()
        {
            return DetectChanges();
        }
    }
}