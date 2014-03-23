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

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using CqlSharp.Linq.Mutations;
using CqlSharp.Linq.Query;
using CqlSharp.Serialization;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A table in a Cassandra Keyspace (database)
    /// </summary>
    /// <typeparam name="TEntity"> type of entities stored in this table </typeparam>
    public class CqlTable<TEntity> : CqlQuery<TEntity>, ICqlTable where TEntity : class, new()
    {
        private readonly CqlContext _context;
        private TableChangeTracker<TEntity> _tracker;

        internal CqlTable(CqlContext context)
            : base(context.QueryProvider)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            _context = context;
        }

        #region ICqlTable Members

        /// <summary>
        ///   Gets the context this table instance belongs to.
        /// </summary>
        /// <value> The context. </value>
        public CqlContext Context
        {
            get { return _context; }
        }

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
                    throw new CqlLinqException("Name of the Table can not be derived for entityType " +
                                               accessor.Type.FullName);

                //if table metadata contains a keyspace, use it
                if (accessor.IsKeySpaceSet)
                    return accessor.Keyspace + "." + accessor.Table;

                //if context database has defined a database, use it
                if (!string.IsNullOrWhiteSpace(_context.Database.Keyspace))
                    return _context.Database.Keyspace + "." + accessor.Table;

                //return the simple table name
                return accessor.Table;
            }
        }

        /// <summary>
        ///   Gets the type of entity contained by this table.
        /// </summary>
        /// <value> The entityType. </value>
        public Type EntityType
        {
            get { return ObjectAccessor<TEntity>.Instance.Type; }
        }

        #endregion

        #region Change Tracking

        private TableChangeTracker<TEntity> ChangeTracker
        {
            get
            {
                if (_tracker == null)
                    _tracker = _context.ChangeTracker.GetTableChangeTracker<TEntity>();

                return _tracker;
            }
        }

        /// <summary>
        ///   Gets the objects that are tracked as part of this table
        /// </summary>
        /// <value> The local. </value>
        public IEnumerable<TEntity> Local
        {
            get { return ChangeTracker.Entities(); }
        }

        /// <summary>
        ///   Adds the specified entity in added state.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        public bool Add(TEntity entity)
        {
            return ChangeTracker.Add(entity);
        }

        /// <summary>
        ///   Attaches the specified entity, in a unmodified state
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public bool Attach(TEntity entity)
        {
            return ChangeTracker.Attach(entity);
        }

        /// <summary>
        ///   Detaches the specified entity fromt the current context.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public bool Detach(TEntity entity)
        {
            return ChangeTracker.Detach(entity);
        }

        /// <summary>
        ///   Marks the specified entity as to-be deleted
        /// </summary>
        /// <param name="entity"> The entity. </param>
        public void Delete(TEntity entity)
        {
            ChangeTracker.Delete(entity);
        }

        /// <summary>
        ///   Adds a range of entities, in an added state.
        /// </summary>
        /// <param name="entities"> The entities. </param>
        public void AddRange(IEnumerable<TEntity> entities)
        {
            foreach (var entity in entities)
                ChangeTracker.Add(entity);
        }

        /// <summary>
        ///   Finds an entity based on the specified key values. If this entity is already
        ///   tracked, the tracked entity is returned (and no database call is made).
        /// </summary>
        /// <param name="keyValues"> The key values. </param>
        /// <returns> </returns>
        public TEntity Find(params object[] keyValues)
        {
            var key = EntityKey<TEntity>.Create(keyValues);

            TEntity entity;
            if (!ChangeTracker.TryGetEntityByKey(key, out entity))
            {
                var connection = _context.Database.Connection;
                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                var query = CqlBuilder<TEntity>.GetSelectQuery(this, key);
                _context.Database.LogQuery(query);

                var command = new CqlCommand(connection, query);
                if (_context.Database.CommandTimeout.HasValue)
                    command.CommandTimeout = _context.Database.CommandTimeout.Value;

                using (var reader = command.ExecuteReader<TEntity>())
                {
                    if (reader.Read())
                    {
                        entity = ChangeTracker.GetOrAttach(reader.Current);
                    }
                }
            }

            return entity;
        }

        /// <summary>
        ///   Finds an entity based on the specified key values. If this entity is already
        ///   tracked, the tracked entity is returned (and no database call is made).
        /// </summary>
        /// <param name="keyValues"> The key values. </param>
        /// <returns> </returns>
        public async Task<TEntity> FindAsync(params object[] keyValues)
        {
            var key = EntityKey<TEntity>.Create(keyValues);

            TEntity entity;
            if (!ChangeTracker.TryGetEntityByKey(key, out entity))
            {
                var connection = _context.Database.Connection;
                if (connection.State == ConnectionState.Closed)
                    await connection.OpenAsync();

                var query = CqlBuilder<TEntity>.GetSelectQuery(this, key);
                _context.Database.LogQuery(query);

                var command = new CqlCommand(connection, query);
                using (var reader = await command.ExecuteReaderAsync<TEntity>())
                {
                    if (await reader.ReadAsync())
                    {
                        entity = ChangeTracker.GetOrAttach(reader.Current);
                    }
                }
            }

            return entity;
        }

        #endregion

        public override string ToString()
        {
            return string.Format("CqlTable<{0}>", ElementType.Name);
        }
    }
}