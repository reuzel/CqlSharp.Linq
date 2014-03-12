using System.Collections.Concurrent;
using System.Collections.Generic;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// Tracks changes for a specific table
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    internal class TableMutationTracker<TEntity> : ITableMutationTracker where TEntity : class, new()
    {
        private readonly CqlTable<TEntity> _table;

        /// <summary>
        ///   The tracked objects
        /// </summary>
        private readonly ConcurrentDictionary<ObjectKey, TrackedObject<TEntity>> _trackedObjects;

        /// <summary>
        ///   Initializes a new instance of the <see cref="MutationTracker" /> class.
        /// </summary>
        internal TableMutationTracker(CqlTable<TEntity> table)
        {
            _table = table;
            _trackedObjects = new ConcurrentDictionary<ObjectKey, TrackedObject<TEntity>>();
        }

        /// <summary>
        ///   Adds the object.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool AddObject(TEntity entity)
        {
            //clone the key values of the entity
            var keyValues = entity.Clone(keyOnly: true);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(_table, entity, keyValues, ObjectState.Added);

            //try to add the object
            return _trackedObjects.TryAdd(ObjectKey.Create(keyValues), entry);
        }

        /// <summary>
        ///   Attaches the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool Attach(TEntity entity)
        {
            //clone the entity, such that changes can be detected (and keys keep unchanged)
            var baseValues = entity.Clone();

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(_table, entity, baseValues, ObjectState.Unchanged);

            //try to add the object
            return _trackedObjects.TryAdd(ObjectKey.Create(baseValues), entry);
        }

        /// <summary>
        ///   Detaches the specified entity.
        /// </summary>
        /// <returns> </returns>
        internal bool Detach(TEntity entity)
        {
            TrackedObject<TEntity> entry;
            return _trackedObjects.TryRemove(ObjectKey.Create(entity), out entry);
        }

        /// <summary>
        ///   Deletes the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        internal void Delete(TEntity entity)
        {
            //clone the key values of the entity
            var keyValues = entity.Clone(keyOnly: true);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(_table, keyValues, default(TEntity), ObjectState.Deleted);

            //set the object to deleted
            _trackedObjects[ObjectKey.Create(keyValues)] = entry;
        }

        /// <summary>
        /// Gets or adds the row as described by the entity. If an entity with an identical key
        /// already is tracked, this already tracked entity is returned
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        internal TEntity GetOrAdd(TEntity entity)
        {
            var key = ObjectKey.Create(entity);
            var trackedObject = _trackedObjects.GetOrAdd(key,
                                     _ =>
                                     new TrackedObject<TEntity>(_table, entity, entity.Clone(), ObjectState.Unchanged));
            return trackedObject.Object;
        }


        /// <summary>
        /// Gets or adds the row as described by the trackedObject. If an trackedobject with an identical key
        /// already is tracked, this already tracked object is returned
        /// </summary>
        /// <param name="trackedObject">The tracked object.</param>
        /// <returns></returns>
        internal TrackedObject<TEntity> GetOrAdd(TrackedObject<TEntity> trackedObject)
        {
            return _trackedObjects.GetOrAdd(ObjectKey.Create(trackedObject.Object), trackedObject);
        }

        /// <summary>
        /// Tries to get a tracked object for a specific key
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="trackedObject">The tracked object.</param>
        /// <returns></returns>
        internal bool TryGetTrackedObject(ObjectKey key, out TrackedObject<TEntity> trackedObject)
        {
            return _trackedObjects.TryGetValue(key, out trackedObject);
        }

        /// <summary>
        /// Gets the tracked table entries.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TrackedObject> Entries()
        {
            return _trackedObjects.Values;
        }


    }
}