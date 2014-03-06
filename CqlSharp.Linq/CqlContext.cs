﻿// CqlSharp.Linq - CqlSharp.Linq
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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using CqlSharp.Linq.Mutations;
using CqlSharp.Linq.Query;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A representation of a Cql database (keyspace)
    /// </summary>
    public abstract class CqlContext : IDisposable
    {
        private string _connectionString;

        /// <summary>
        ///   The list of tables known to this context
        /// </summary>
        private readonly ConcurrentDictionary<Type, ICqlTable> _tables;

        /// <summary>
        ///   Gets the CQL query provider.
        /// </summary>
        /// <value> The CQL query provider. </value>
        internal CqlQueryProvider CqlQueryProvider { get; private set; }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlContext" /> class.
        /// </summary>
        /// <param name="initializeTables"> indicates wether the table properties are to be automatically initialized </param>
        protected CqlContext(bool initializeTables = true)
        {
#if DEBUG
            SkipExecute = false;
#endif
            _tables = new ConcurrentDictionary<Type, ICqlTable>();
            MutationTracker = new MutationTracker();
            CqlQueryProvider = new CqlQueryProvider(this);

            if (initializeTables)
                InitializeTables();
        }

        /// <summary>
        ///   Initializes a new instance of the <see cref="CqlContext" /> class.
        /// </summary>
        /// <param name="connectionString"> The connection string. </param>
        /// <param name="initializeTables"> indicates wether the table properties are to be automatically initialized </param>
        protected CqlContext(string connectionString, bool initializeTables = true)
            : this(initializeTables)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        ///   Gets the connection string.
        /// </summary>
        /// <value> The connection string. </value>
        public string ConnectionString
        {
            get
            {
                if (_connectionString == null)
                    _connectionString = GetType().Name;

                return _connectionString;
            }

            set { _connectionString = value; }
        }

        /// <summary>
        ///   Gets or sets the log where executed CQL queries are written to
        /// </summary>
        /// <value> The log. </value>
        public Action<string> Log { get; set; }

#if DEBUG
        /// <summary>
        ///   Gets or sets a value indicating whether execution of the query is skipped. This is for debugging purposes.
        /// </summary>
        /// <value> <c>true</c> if execution is skipped; otherwise, <c>false</c> . </value>
        public bool SkipExecute { get; set; }
#endif

        #region IDisposable Members

        /// <summary>
        ///   Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
        }

        #endregion

        /// <summary>
        ///   Initializes the tables of this context.
        /// </summary>
        private void InitializeTables()
        {
            var properties = GetType().GetProperties();
            foreach (var property in properties)
            {
                var propertyType = property.PropertyType;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof (CqlTable<>))
                {
                    //create new table object
                    var table =
                        (ICqlTable)
                        Activator.CreateInstance(propertyType, BindingFlags.NonPublic | BindingFlags.Instance, null,
                                                 new object[] {this}, null);

                    //add it to the list of known tables
                    table = _tables.GetOrAdd(table.Type, table);

                    //set the property
                    property.SetValue(this, table);
                }
            }
        }

        /// <summary>
        ///   Gets the table represented by the provided type.
        /// </summary>
        /// <typeparam name="T"> type that represents the values in the table </typeparam>
        /// <returns> a CqlTable </returns>
        public CqlTable<T> GetTable<T>() where T : class, new()
        {
            return (CqlTable<T>) _tables.GetOrAdd(typeof (T), new CqlTable<T>(this));
        }

        /// <summary>
        ///   Gets the mutation tracker.
        /// </summary>
        /// <value> The mutation tracker. </value>
        public MutationTracker MutationTracker { get; private set; }

        /// <summary>
        ///   Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="consistency"> The consistency level. Defaults to one. </param>
        public void SaveChanges(CqlConsistency consistency = CqlConsistency.One)
        {
            if (MutationTracker.HasChanges())
            {
                using (var connection = new CqlConnection(ConnectionString))
                {
                    connection.Open();

                    foreach (var trackedObject in MutationTracker.Entries())
                    {
                        if (trackedObject.State != ObjectState.Unchanged)
                        {
                            var command = new CqlCommand(connection, trackedObject.GetDmlStatement(), consistency);
                            command.PartitionKey.Set(trackedObject.Object);
                            command.ExecuteNonQueryAsync();
                            trackedObject.SetOriginalValues(trackedObject.Object);
                        }
                    }
                }
            }
        }

        /// <summary>
        ///   Saves the changes with the required consistency level.
        /// </summary>
        /// <param name="cancellationToken"> the cancellation token </param>
        /// <param name="consistency"> The consistency level. Defaults to one. </param>
        public async Task SaveChangesAsync(CancellationToken cancellationToken,
                                           CqlConsistency consistency = CqlConsistency.One)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (MutationTracker.HasChanges())
            {
                using (var connection = new CqlConnection(ConnectionString))
                {
                    await connection.OpenAsync(cancellationToken);

                    foreach (var trackedObject in MutationTracker.Entries())
                    {
                        if (trackedObject.State != ObjectState.Unchanged)
                        {
                            var command = new CqlCommand(connection, trackedObject.GetDmlStatement(), consistency);
                            command.PartitionKey.Set(trackedObject.Object);
                            await command.ExecuteNonQueryAsync(cancellationToken);
                            trackedObject.SetOriginalValues(trackedObject.Object);
                        }
                    }
                }
            }
        }
    }
}