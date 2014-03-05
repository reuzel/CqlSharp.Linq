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
    ///   Tracks the mutation of a specific table
    /// </summary>
    public class MutationTracker
    {
        /// <summary>
        ///   The tracked objects
        /// </summary>
        private readonly ConcurrentDictionary<ObjectKey, TrackedObject> _trackedObjects;

        /// <summary>
        ///   Initializes a new instance of the <see cref="MutationTracker" /> class.
        /// </summary>
        internal MutationTracker()
        {
            _trackedObjects = new ConcurrentDictionary<ObjectKey, TrackedObject>();
        }


        /// <summary>
        ///   Adds a new tracked entry to the mutation tracking list
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <param name="trackedObject"> The tracked object. </param>
        /// <returns> </returns>
        internal bool AddEntry<TEntity>(TrackedObject<TEntity> trackedObject)
        {
            var key = ObjectKey.Create(trackedObject.Original);
            return _trackedObjects.TryAdd(key, trackedObject);
        }

        /// <summary>
        ///   Adds the object.
        /// </summary>
        /// <param name="table"> table to which entity belongs </param>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool AddObject<TEntity>(CqlTable<TEntity> table, TEntity entity)
        {
            //clone the key values of the entity
            var keyValues = entity.Clone(keyOnly: true);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(table, entity, keyValues, ObjectState.Added);

            //try to add the object
            return _trackedObjects.TryAdd(ObjectKey.Create(keyValues), entry);
        }

        /// <summary>
        ///   Attaches the specified entity.
        /// </summary>
        /// <param name="table"> table to which entity belongs </param>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool Attach<TEntity>(CqlTable<TEntity> table, TEntity entity)
        {
            //clone the entity, such that changes can be detected (and keys keep unchanged)
            var baseValues = entity.Clone();

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(table, entity, baseValues, ObjectState.Unchanged);

            //try to add the object
            return _trackedObjects.TryAdd(ObjectKey.Create(baseValues), entry);
        }

        /// <summary>
        ///   Detaches the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool Detach<TEntity>(TEntity entity)
        {
            TrackedObject entry;
            return _trackedObjects.TryRemove(ObjectKey.Create(entity), out entry);
        }

        /// <summary>
        ///   Deletes the specified entity.
        /// </summary>
        /// <param name="table"> the table to which the entity belongs </param>
        /// <param name="entity"> The entity. </param>
        internal void Delete<TEntity>(CqlTable<TEntity> table, TEntity entity)
        {
            //clone the key values of the entity
            var keyValues = entity.Clone(keyOnly: true);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(table, keyValues, default(TEntity), ObjectState.Deleted);

            //set the object to deleted
            _trackedObjects[ObjectKey.Create(keyValues)] = entry;
        }

        /// <summary>
        ///   Gets the changed objects.
        /// </summary>
        /// <returns> </returns>
        public IEnumerable<TrackedObject> Entries()
        {
            return _trackedObjects.Values;
        }

        /// <summary>
        ///   Gets the changed objects of a certain type.
        /// </summary>
        /// <returns> </returns>
        public IEnumerable<TrackedObject<TEntity>> Entries<TEntity>()
        {
            return _trackedObjects.Values.OfType<TrackedObject<TEntity>>();
        }

        /// <summary>
        ///   Tries to the get tracked object.
        /// </summary>
        /// <param name="key"> The key. </param>
        /// <param name="trackedObject"> The tracked object. </param>
        /// <returns> true if an object with a certain key was tracked </returns>
        internal bool TryGetTrackedObject(ObjectKey key, out TrackedObject trackedObject)
        {
            return _trackedObjects.TryGetValue(key, out trackedObject);
        }

        /// <summary>
        ///   Detects any changes made to tracked objects
        /// </summary>
        /// <returns> true if any object contains a change to be committed to the server </returns>
        public bool DetectChanges()
        {
            bool hasChanges = false;
            foreach (var trackedObject in _trackedObjects.Values)
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