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

using CqlSharp.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Tracks changes to a single object
    /// </summary>
    public abstract class TrackedObject
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="TrackedObject{TEntity}" /> class.
        /// </summary>
        /// <param name="table"> </param>
        /// <param name="entity"> The entity. </param>
        /// <param name="original"> </param>
        /// <param name="objectState"> The State. </param>
        protected TrackedObject(ICqlTable table, Object entity, Object original, ObjectState objectState)
        {
            if (table == null) throw new ArgumentNullException("table");
            if (entity == null) throw new ArgumentNullException("entity");

            if (objectState == ObjectState.Modified)
                throw new ArgumentException(
                    "Can't start tracked a object in Modified, as it is not known what was changed", "objectState");

            State = objectState;
            Table = table;
            Object = entity;
            Original = original;
            ChangedColumns = null;
        }

        /// <summary>
        ///   Gets or sets the state.
        /// </summary>
        /// <value> The state. </value>
        public ObjectState State { get; protected set; }

        /// <summary>
        ///   the columns that were changed
        /// </summary>
        public IEnumerable<ICqlColumnInfo> ChangedColumns { get; protected set; }

        /// <summary>
        ///   Gets the object being tracked
        /// </summary>
        /// <value> The object. </value>
        public Object Object { get; private set; }

        /// <summary>
        ///   Gets the original values of the object
        /// </summary>
        /// <value> The object. </value>
        public Object Original { get; protected set; }

        /// <summary>
        ///   Gets the table.
        /// </summary>
        /// <value> The table. </value>
        public ICqlTable Table { get; private set; }

        /// <summary>
        ///   Detects if the tracked object has changed state
        /// </summary>
        /// <returns> </returns>
        internal abstract bool DetectChanges();

        /// <summary>
        ///   Gets the DML Cql statement describing the required changes.
        /// </summary>
        /// <returns> </returns>
        internal abstract string GetDmlStatement();

        /// <summary>
        ///   Reloads the entity from the database. If the entity was found, the database
        ///   values will be copied into the original and object values, resulting in a 
        ///   Unchanged state. When the entity was not found in the database, the entity
        ///   will enter the Detached state.
        /// </summary>
        public abstract void Reload();

        /// <summary>
        /// Sets the object values.
        /// </summary>
        /// <param name="newValues">The new values.</param>
        public abstract void SetObjectValues(object newValues);

        /// <summary>
        /// Sets the original values.
        /// </summary>
        /// <param name="newOriginal">The new original.</param>
        public abstract void SetOriginalValues(object newOriginal);
    }

    /// <summary>
    ///   Tracks the changes to a single object of a specific type
    /// </summary>
    /// <typeparam name="TEntity"> The type of the entity. </typeparam>
    public class TrackedObject<TEntity> : TrackedObject where TEntity : class, new()
    {
        /// <summary>
        ///   Initializes a new instance of the <see cref="TrackedObject{TEntity}" /> class.
        /// </summary>
        /// <param name="table"> The table. </param>
        /// <param name="entity"> The entity. </param>
        /// <param name="original"> The original. </param>
        /// <param name="objectState"> State of the object. </param>
        public TrackedObject(ICqlTable table, TEntity entity, TEntity original, ObjectState objectState)
            : base(table, entity, original, objectState)
        {
        }

        /// <summary>
        /// Gets the reload CQL statement.
        /// </summary>
        /// <returns></returns>
        private string GetReloadCql()
        {
            var accessor = ObjectAccessor<TEntity>.Instance;

            var sb = new StringBuilder();
            sb.Append("SELECT ");

            bool firstColumn = true;
            foreach (var column in accessor.Columns)
            {
                if (!firstColumn)
                {
                    sb.Append(",");
                }
                sb.Append(" \"");
                sb.Append(column.Name);
                sb.Append("\"");
                firstColumn = false;
            }
            sb.Append(" FROM \"");
            sb.Append(Table.Name);
            sb.Append("\" WHERE");

            firstColumn = true;
            foreach (var keyColumn in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (!firstColumn)
                    sb.Append(" AND ");
                sb.Append(" \"");
                sb.Append(keyColumn.Name);
                sb.Append("\"=");
                var value = keyColumn.ReadFunction(Object);
                sb.Append(TypeSystem.ToStringValue(value, keyColumn.CqlType));
                firstColumn = false;
            }

            return sb.ToString();
        }
        /// <summary>
        ///   Gets the object being tracked
        /// </summary>
        /// <value> The object. </value>
        public new TEntity Object
        {
            get { return (TEntity)base.Object; }
        }

        /// <summary>
        ///   Gets the original values of the object
        /// </summary>
        /// <value> The object. </value>
        public new TEntity Original
        {
            get { return (TEntity)base.Original; }
            protected set { base.Original = value; }
        }

        /// <summary>
        ///   Detects the changes.
        /// </summary>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">Illegal change detected: A tracked object has changed its key</exception>
        internal override bool DetectChanges()
        {
            if (State == ObjectState.Detached)
                return false;

            if (State == ObjectState.Deleted)
                return true;

            //make sure the entity did not switch key
            var originalKey = ObjectKey.Create(Original);
            var entityKey = ObjectKey.Create(Object);
            if (!originalKey.Equals(entityKey))
                throw new CqlLinqException("Illegal change detected: A tracked object has changed its key");

            if (State == ObjectState.Added)
                return true;

            //find which columns have changed
            var changedColumns = new List<CqlColumnInfo<TEntity>>();
            foreach (CqlColumnInfo<TEntity> column in ObjectAccessor<TEntity>.Instance.NormalColumns)
            {
                if (column.ReadFunction != null)
                {
                    var original = column.ReadFunction(Original);
                    var actual = column.ReadFunction(Object);

                    if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                    {
                        if (!TypeSystem.SequenceEqual((IEnumerable)original, (IEnumerable)actual))
                            changedColumns.Add(column);
                    }
                    else if (!Equals(original, actual))
                    {
                        changedColumns.Add(column);
                    }
                }
            }

            //update property
            ChangedColumns = changedColumns;

            //update state
            if (changedColumns.Count > 0)
            {
                State = ObjectState.Modified;
                return true;
            }

            State = ObjectState.Unchanged;
            return false;
        }

        internal override string GetDmlStatement()
        {
            return CqlDmlBuilder<TEntity>.BuildDmlQuery(this);
        }

        /// <summary>
        ///   Sets the original values
        /// </summary>
        /// <param name="newOriginal"> The values to use as original values, which should represent the known database state </param>
        public override void SetOriginalValues(object newOriginal)
        {
            var newOriginalEntity = (TEntity)newOriginal;
            if (ObjectKey.Create(newOriginalEntity) != ObjectKey.Create(Object))
                throw new ArgumentException(
                    "The new original values represent an different entity than the one tracked. The key values do not match",
                    "newOriginal");

            Original = newOriginalEntity.Clone();
        }

        /// <summary>
        ///   Sets object values
        /// </summary>
        /// <param name="newValues"> The values to use as object values, which should represent the new (uncommitted) database state </param>
        public override void SetObjectValues(object newValues)
        {
            var newValuesEntity = (TEntity)newValues;
            if (ObjectKey.Create(newValuesEntity) != ObjectKey.Create(Original))
                throw new ArgumentException(
                    "The new object values represent an different entity than the one tracked. The key values do not match",
                    "newValues");

            newValuesEntity.CopyTo(Object);
        }

        /// <summary>
        ///   Reloads this instance.
        /// </summary>
        public override void Reload()
        {
            using (var connection = new CqlConnection(Table.Context.ConnectionString))
            {
                connection.Open();

                var cql = GetReloadCql();
                if(Table.Context.Log!=null)Table.Context.Log(cql);

                var command = new CqlCommand(connection, cql);

                using (var reader = command.ExecuteReader<TEntity>())
                {
                    if (reader.Read())
                    {
                        var row = reader.Current;
                        SetOriginalValues(row);
                        SetObjectValues(row);
                        State = ObjectState.Unchanged;
                    }
                    else
                    {
                        State = ObjectState.Detached;
                    }
                }
            }
        }
    }
}