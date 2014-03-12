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
    /// <typeparam name="T"> </typeparam>
    public class CqlTable<T> : CqlQuery<T>, ICqlTable where T : class, new()
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
        public IEnumerable<T> Local
        {
            get { return _context.MutationTracker.GetTracker(this).Entries().Select(to => (T)to.Object); }
        }

        public bool Attach(T entity)
        {
            return _context.MutationTracker.GetTracker(this).Attach(entity);
        }

        public bool Detach(T entity)
        {
            return _context.MutationTracker.GetTracker(this).Detach(entity);
        }

        public void Delete(T entity)
        {
            _context.MutationTracker.GetTracker(this).Delete(entity);
        }

        public void Add(T entity)
        {
            _context.MutationTracker.GetTracker(this).AddObject(entity);
        }

        public void AddRange(IEnumerable<T> entities)
        {
            TableMutationTracker<T> mutationTracker = _context.MutationTracker.GetTracker(this);
            foreach (var entity in entities)
                mutationTracker.AddObject(entity);
        }

        public T Find(params object[] keyValues)
        {
            var key = ObjectKey.Create<T>(keyValues);

            var tracker = _context.MutationTracker.GetTracker(this);

            TrackedObject<T> trackedObject;
            if (!tracker.TryGetTrackedObject(key, out trackedObject))
            {
                //object not found, create a tracked object
                trackedObject = new TrackedObject<T>(this, key.GetKeyValues<T>(), default(T), ObjectState.Detached);

                //load any existing values from the database
                trackedObject.Reload();

                //add the object, or return existing one if we were raced
                trackedObject = tracker.GetOrAdd(trackedObject);
            }

            //check state
            if (trackedObject.State != ObjectState.Detached)
                return trackedObject.Object;

            return default(T);
        }



        #endregion

        #region ICqlTable Members

        /// <summary>
        ///   Gets the column names.
        /// </summary>
        /// <value> The column names. </value>
        public IEnumerable<ICqlColumnInfo> Columns
        {
            get { return ObjectAccessor<T>.Instance.Columns; }
        }

        /// <summary>
        ///   Gets the name of the Table.
        /// </summary>
        /// <value> The name. </value>
        public string Name
        {
            get
            {
                var accessor = ObjectAccessor<T>.Instance;

                if (!accessor.IsTableSet)
                    throw new CqlLinqException("Name of the Table can not be derived for type " + accessor.Type.FullName);

                if (accessor.IsKeySpaceSet)
                    return accessor.Keyspace + "." + accessor.Table;

                return accessor.Table;
            }
        }

        /// <summary>
        ///   Gets the type of entity contained by this table.
        /// </summary>
        /// <value> The type. </value>
        public Type Type
        {
            get { return ObjectAccessor<T>.Instance.Type; }
        }

        #endregion

        public override string ToString()
        {
            return string.Format("CqlTable<{0}>", ElementType.Name);
        }
    }
}