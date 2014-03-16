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

        //todo: table per entity to check if values are already added or not

        /// <summary>
        ///   The tracked objects
        /// </summary>
        private readonly ConcurrentDictionary<EntityKey<TEntity>, TrackedEntity<TEntity>> _trackedObjects;

        /// <summary>
        ///   Initializes a new instance of the <see cref="MutationTracker" /> class.
        /// </summary>
        internal TableMutationTracker(CqlTable<TEntity> table)
        {
            _table = table;
            _trackedObjects = new ConcurrentDictionary<EntityKey<TEntity>, TrackedEntity<TEntity>>();
        }

        /// <summary>
        ///   Adds the entity in an Added state.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool Add(TEntity entity)
        {
            //create a new tracked object
            var entry = new TrackedEntity<TEntity>(_table, entity, default(TEntity), EntityState.Added);

            //try to add the object
            return _trackedObjects.TryAdd(EntityKey<TEntity>.Create(entity), entry);
        }

        /// <summary>
        ///   Attaches the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        internal bool Attach(TEntity entity)
        {
            //create a new tracked object
            var entry = new TrackedEntity<TEntity>(_table, entity, entity.Clone(), EntityState.Unchanged);

            //try to add the object
            return _trackedObjects.TryAdd(EntityKey<TEntity>.Create(entity), entry);
        }

        /// <summary>
        ///   Detaches the specified entity.
        /// </summary>
        /// <returns> </returns>
        internal bool Detach(TEntity entity)
        {
            TrackedEntity<TEntity> entry;
            return _trackedObjects.TryRemove(EntityKey<TEntity>.Create(entity), out entry);

            //check if original entity reference is the one tracked?
        }

        /// <summary>
        ///   Deletes the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        internal void Delete(TEntity entity)
        {
            //create a new tracked object
            var entry = new TrackedEntity<TEntity>(_table, entity, default(TEntity), EntityState.Deleted);

            //set the object to deleted
            _trackedObjects[EntityKey<TEntity>.Create(entity)] = entry;
        }

        /// <summary>
        /// Gets or adds the row as described by the entity. If an entity with an identical key
        /// already is tracked, this already tracked entity is returned
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        internal TEntity GetOrAdd(TEntity entity)
        {
            var key = EntityKey<TEntity>.Create(entity);

            var trackedObject = _trackedObjects.GetOrAdd(key,
                                     _ => new TrackedEntity<TEntity>(_table, entity, entity.Clone(), EntityState.Unchanged));

            return trackedObject.Object;
        }


        /// <summary>
        /// Gets or adds the row as described by the TrackedEntity. If an trackedobject with an identical key
        /// already is tracked, this already tracked object is returned
        /// </summary>
        /// <param name="trackedEntity">The tracked object.</param>
        /// <returns></returns>
        internal TrackedEntity<TEntity> GetOrAdd(TrackedEntity<TEntity> trackedEntity)
        {
            return _trackedObjects.GetOrAdd(EntityKey<TEntity>.Create(trackedEntity.Object), trackedEntity);
        }

        /// <summary>
        /// Tries to get a tracked object for a specific key
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="trackedEntity">The tracked object.</param>
        /// <returns></returns>
        internal bool TryGetTrackedObject(EntityKey<TEntity> key, out TrackedEntity<TEntity> trackedEntity)
        {
            return _trackedObjects.TryGetValue(key, out trackedEntity);
        }

        /// <summary>
        /// Gets the tracked table entries.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TrackedEntity> Entries()
        {
            return _trackedObjects.Values;
        }


    }
}