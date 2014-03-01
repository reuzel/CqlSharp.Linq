using CqlSharp.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// Tracks the mutation of a specific table
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    internal class MutationTracker<TEntity> : IMutationTracker
    {
        private readonly ConcurrentDictionary<ObjectKey<TEntity>, TrackedObject<TEntity>> _trackedObjects;
        private readonly CqlTable<TEntity> _table;

        /// <summary>
        /// Initializes a new instance of the <see cref="MutationTracker{TEntity}"/> class.
        /// </summary>
        /// <param name="table">The table.</param>
        public MutationTracker(CqlTable<TEntity> table)
        {
            _table = table;
            _trackedObjects = new ConcurrentDictionary<ObjectKey<TEntity>, TrackedObject<TEntity>>();
        }

        /// <summary>
        /// Adds the object.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public bool AddObject(TEntity entity)
        {
            //clone the key values of the entity
            var keyValues = Clone(entity, true);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(_table, entity, keyValues, ObjectState.Added);

            //try to add the object
            return _trackedObjects.TryAdd(new ObjectKey<TEntity>(keyValues), entry);
        }

        /// <summary>
        /// Attaches the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public bool Attach(TEntity entity)
        {
            //clone the entity, such that changes can be detected (and keys keep unchanged)
            var baseValues = Clone(entity);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(_table, entity, baseValues, ObjectState.Unchanged);

            //try to add the object
            return _trackedObjects.TryAdd(new ObjectKey<TEntity>(baseValues), entry);
        }

        /// <summary>
        /// Detaches the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public bool Detach(TEntity entity)
        {
            TrackedObject<TEntity> entry;
            return _trackedObjects.TryRemove(new ObjectKey<TEntity>(entity), out entry);
        }

        /// <summary>
        /// Deletes the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void Delete(TEntity entity)
        {
            //clone the key values of the entity
            var keyValues = Clone(entity, true);

            //create a new tracked object
            var entry = new TrackedObject<TEntity>(_table, keyValues, default(TEntity), ObjectState.Deleted);

            //set the object to deleted
            _trackedObjects[new ObjectKey<TEntity>(keyValues)] = entry;
        }

        /// <summary>
        /// Gets the changed objects.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<ITrackedObject> GetChangedObjects()
        {
            return _trackedObjects.Values.Where(entry => entry.DetectChanges());
        }

        /// <summary>
        /// Clones the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="keyOnly">clones the key values only</param>
        /// <returns></returns>
        private TEntity Clone(TEntity entity, bool keyOnly = false)
        {
            var clone = (TEntity)Activator.CreateInstance(typeof(TEntity), BindingFlags.Instance | BindingFlags.Public);
            foreach (CqlColumnInfo<TEntity> column in ObjectAccessor<TEntity>.Instance.Columns)
            {
                //make sure value can be cloned
                if (column.ReadFunction == null || column.WriteFunction == null) continue;

                //skip if column is not a key column, and only keys need to be cloned
                if (keyOnly && !column.IsPartitionKey && !column.IsClusteringKey)
                    continue;

                //get the value
                var value = column.ReadFunction(entity);

                //mandate that all key values are set
                if (value == null && (column.IsPartitionKey || column.IsClusteringKey))
                    throw new CqlLinqException("Can not track an object who's key values are not completely set");

                //if the column is a collection type, clone the collection
                if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                {
                    value = Activator.CreateInstance(column.Type, value);
                }

                //copy the value into the clone
                column.WriteFunction(clone, value);
            }

            return clone;
        }
    }
}
