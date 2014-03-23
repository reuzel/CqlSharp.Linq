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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Tracks the mutation accross all tables in a context
    /// </summary>
    public class CqlChangeTracker
    {
        private readonly CqlContext _context;

        /// <summary>
        ///   The table mutation trackers
        /// </summary>
        private readonly ConcurrentDictionary<Type, ITableChangeTracker> _tableTrackers;

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlChangeTracker" /> class.
        /// </summary>
        internal CqlChangeTracker(CqlContext context)
        {
            _context = context;
            _tableTrackers = new ConcurrentDictionary<Type, ITableChangeTracker>();
            AutoDetectChangesEnabled = true;
        }

        /// <summary>
        ///   Gets or sets a value indicating whether the DetectChanges() method is called automatically by methods of CqlContext and related classes. The default value is true
        /// </summary>
        /// <value> true if should be called automatically; otherwise, false. </value>
        public bool AutoDetectChangesEnabled { get; set; }

        /// <summary>
        ///   Gets the tracker for the given table.
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <returns> </returns>
        internal TableChangeTracker<TEntity> GetTableChangeTracker<TEntity>() where TEntity : class, new()
        {
            return
                (TableChangeTracker<TEntity>)
                _tableTrackers.GetOrAdd(typeof(TEntity),
                                        t => new TableChangeTracker<TEntity>(_context.GetTable<TEntity>()));
        }

        /// <summary>
        ///   Returns the tracked entries in the scope of the related context
        /// </summary>
        /// <returns> </returns>
        public IEnumerable<ITrackedEntity> Entries()
        {
            return _tableTrackers.Values.SelectMany(tt => tt.Entries());
        }

        /// <summary>
        ///   Returns the tracked entries of a specific type
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <returns> </returns>
        public IEnumerable<TrackedEntity<TEntity>> Entries<TEntity>() where TEntity : class, new()
        {
            ITableChangeTracker tableChangeTracker;
            if (_tableTrackers.TryGetValue(typeof(TEntity), out tableChangeTracker))
            {
                var tracker = (TableChangeTracker<TEntity>)tableChangeTracker;
                return tracker.Entries();
            }

            return Enumerable.Empty<TrackedEntity<TEntity>>();
        }

        /// <summary>
        /// Gets the entry for the given entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <param name="entry">The entry, if the return value is true</param>
        /// <returns>
        /// true if the entity is tracked for changes
        /// </returns>
        public bool TryGetEntry(Object entity, out ITrackedEntity entry)
        {
            ITableChangeTracker tableChangeTracker;
            if (_tableTrackers.TryGetValue(entity.GetType(), out tableChangeTracker))
            {
                return tableChangeTracker.TryGetEntry(entity, out entry);
            }

            entry = null;
            return false;
        }

        /// <summary>
        /// Gets the entry for the given entity.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="entity">The entity.</param>
        /// <param name="entry">The entry, if the return value is true</param>
        /// <returns>true if the entity is tracked for changes</returns>
        public bool TryGetEntry<TEntity>(TEntity entity, out TrackedEntity<TEntity> entry) where TEntity : class, new()
        {
            ITableChangeTracker tableChangeTracker;
            if (_tableTrackers.TryGetValue(entity.GetType(), out tableChangeTracker))
            {
                var tracker = (TableChangeTracker<TEntity>)tableChangeTracker;
                return tracker.TryGetEntry(entity, out entry);
            }

            entry = null;
            return false;
        }

        /// <summary>
        ///   Detects any changes made to tracked objects
        /// </summary>
        /// <returns> true if any object contains a change to be committed to the server </returns>
        public bool DetectChanges()
        {
            bool hasChanges = false;
            // ReSharper disable LoopCanBeConvertedToQuery
            foreach (var tracker in _tableTrackers.Values)
            {
                hasChanges |= tracker.DetectChanges();
            }
            // ReSharper restore LoopCanBeConvertedToQuery
            return hasChanges;
        }

        /// <summary>
        ///   Detects any changes made to tracked objects
        /// </summary>
        /// <returns> true if any object contains a change to be committed to the server </returns>
        public bool HasChanges()
        {
            if (AutoDetectChangesEnabled)
                return DetectChanges();

            return _tableTrackers.Values.Any(tt => tt.HasChanges());
        }

        /// <summary>
        /// Saves the changes.
        /// </summary>
        /// <param name="consistency">The consistency.</param>
        /// <param name="acceptChangesDuringSave">if set to <c>true</c> [accept changes during save].</param>
        internal void SaveChanges(CqlConsistency consistency, bool acceptChangesDuringSave)
        {
            //get the connection, and open it
            var connection = _context.Database.Connection;
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            //get the existing transaction, or create a temporary one if none added
            bool ownsTransaction;
            CqlBatchTransaction transaction;
            if (_context.Database.CurrentTransaction == null)
            {
                transaction = connection.BeginTransaction();
                if (_context.Database.CommandTimeout.HasValue)
                    transaction.CommandTimeout = _context.Database.CommandTimeout.Value;
                transaction.Consistency = consistency;
                ownsTransaction = true;
            }
            else
            {
                transaction = _context.Database.CurrentTransaction;
                ownsTransaction = false;
            }

            try
            {
                //detect changes made to entities
                if (AutoDetectChangesEnabled) DetectChanges();

                //enlist the existing changes
                foreach (var tracker in _tableTrackers.Values)
                {
                    tracker.EnlistChanges(connection, transaction, consistency);
                }

                //commit if we own the transaction
                if (ownsTransaction)
                    transaction.Commit();

                //accept changes
                if (acceptChangesDuringSave)
                    AcceptAllChanges();
            }
            finally
            {
                if (ownsTransaction)
                    transaction.Dispose();
            }
        }

        /// <summary>
        /// Saves the changes asynchronous.
        /// </summary>
        /// <param name="consistency">The consistency.</param>
        /// <param name="acceptChangesDuringSave">if set to <c>true</c> [accept changes during save].</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns></returns>
        internal async Task SaveChangesAsync(CqlConsistency consistency, bool acceptChangesDuringSave, CancellationToken cancellationToken)
        {
            //quit when cancelled from the start
            cancellationToken.ThrowIfCancellationRequested();

            //get the connection, and open it
            var connection = _context.Database.Connection;
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            //get the existing transaction, or create a temporary one if none added
            bool ownsTransaction;
            CqlBatchTransaction transaction;
            if (_context.Database.CurrentTransaction == null)
            {
                transaction = connection.BeginTransaction();
                transaction.Consistency = consistency;
                ownsTransaction = true;
            }
            else
            {
                transaction = _context.Database.CurrentTransaction;
                ownsTransaction = false;
            }

            try
            {
                //detect changes made to entities
                if (AutoDetectChangesEnabled) DetectChanges();

                //enlist changes to the transaction
                foreach (var tracker in _tableTrackers.Values)
                {
                    tracker.EnlistChanges(connection, transaction, consistency);
                }

                //commit if we own the transaction
                if (ownsTransaction)
                    await transaction.CommitAsync(cancellationToken);

                //accept changes
                if (acceptChangesDuringSave)
                    AcceptAllChanges();
            }
            finally
            {
                if (ownsTransaction)
                    transaction.Dispose();
            }
        }

        /// <summary>
        /// Accepts all changes.
        /// </summary>
        internal void AcceptAllChanges()
        {
            //enlist changes to the transaction
            foreach (var tracker in _tableTrackers.Values)
            {
                tracker.AcceptAllChanges();
            }
        }
    }
}