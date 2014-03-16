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

using CqlSharp.Linq.Mutations;
using CqlSharp.Linq.Query;
using CqlSharp.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A table in a Cassandra Keyspace (database)
    /// </summary>
    /// <typeparam name="TEntity">type of entities stored in this table</typeparam>
    public class CqlTable<TEntity> : CqlQuery<TEntity>, ICqlTable where TEntity : class, new()
    {
        private readonly CqlContext _context;

        internal CqlTable(CqlContext context)
            : base(context.CqlQueryProvider)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            _context = context;
        }

        /// <summary>
        /// Gets the context this table instance belongs to.
        /// </summary>
        /// <value>
        /// The context.
        /// </value>
        public CqlContext Context
        {
            get { return _context; }
        }

        #region Change Tracking

        /// <summary>
        /// Gets the objects that are tracked as part of this table
        /// </summary>
        /// <value>
        /// The local.
        /// </value>
        public IEnumerable<TEntity> Local
        {
            get { return _context.MutationTracker.GetTracker(this).Entries().Select(to => (TEntity)to.Object); }
        }

        /// <summary>
        /// Adds the specified entity in added state.
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void Add(TEntity entity)
        {
            _context.MutationTracker.GetTracker(this).Add(entity);
        }

        /// <summary>
        /// Attaches the specified entity, in a unmodified state
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public bool Attach(TEntity entity)
        {
            return _context.MutationTracker.GetTracker(this).Attach(entity);
        }

        /// <summary>
        /// Detaches the specified entity fromt the current context.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public bool Detach(TEntity entity)
        {
            return _context.MutationTracker.GetTracker(this).Detach(entity);
        }

        /// <summary>
        /// Marks the specified entity as to-be deleted
        /// </summary>
        /// <param name="entity">The entity.</param>
        public void Delete(TEntity entity)
        {
            _context.MutationTracker.GetTracker(this).Delete(entity);
        }

        /// <summary>
        /// Adds a range of entities, in an added state.
        /// </summary>
        /// <param name="entities">The entities.</param>
        public void AddRange(IEnumerable<TEntity> entities)
        {
            TableMutationTracker<TEntity> mutationTracker = _context.MutationTracker.GetTracker(this);
            foreach (var entity in entities)
                mutationTracker.Add(entity);
        }

        /// <summary>
        /// Finds an entity based on the specified key values. If this entity is already
        /// tracked, the tracked entity is returned (and no database call is made).
        /// </summary>
        /// <param name="keyValues">The key values.</param>
        /// <returns></returns>
        public TEntity Find(params object[] keyValues)
        {
            var key = EntityKey<TEntity>.Create(keyValues);

            var tracker = _context.MutationTracker.GetTracker(this);

            TrackedEntity<TEntity> trackedEntity;
            if (!tracker.TryGetTrackedObject(key, out trackedEntity))
            {
                //object not found, create a tracked object
                trackedEntity = new TrackedEntity<TEntity>(this, key.GetKeyValues(), default(TEntity), EntityState.Detached);

                //load any existing values from the database
                trackedEntity.Reload();

                //add the object, or return existing one if we were raced
                trackedEntity = tracker.GetOrAdd(trackedEntity);
            }

            //check state
            if (trackedEntity.State != EntityState.Detached)
                return trackedEntity.Object;

            return default(TEntity);
        }



        #endregion

        #region ICqlTable Members

        /// <summary>
        ///   Gets the column names.
        /// </summary>
        /// <value> The column names. </value>
        public IEnumerable<ICqlColumnInfo> Columns
        {
            get { return ObjectAccessor<TEntity>.Instance.Columns; }
        }

        /// <summary>
        ///   Gets the name of the Table.
        /// </summary>
        /// <value> The name. </value>
        public string Name
        {
            get
            {
                var accessor = ObjectAccessor<TEntity>.Instance;

                if (!accessor.IsTableSet)
                    throw new CqlLinqException("Name of the Table can not be derived for entityType " + accessor.Type.FullName);

                if (accessor.IsKeySpaceSet)
                    return accessor.Keyspace + "." + accessor.Table;

                return accessor.Table;
            }
        }

        /// <summary>
        ///   Gets the type of entity contained by this table.
        /// </summary>
        /// <value> The entityType. </value>
        public Type Type
        {
            get { return ObjectAccessor<TEntity>.Instance.Type; }
        }

        #endregion

        public override string ToString()
        {
            return string.Format("CqlTable<{0}>", ElementType.Name);
        }
    }
}