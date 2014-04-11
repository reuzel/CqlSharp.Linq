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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;

namespace CqlSharp.Linq.Query
{
    internal class TrackingReader<TEntity> : IEnumerable<TEntity>, IProjectionReader where TEntity : class, new()
    {
        private readonly CqlContext _context;
        private readonly QueryPlan _plan;
        private readonly object[] _args;

        /// <summary>
        /// Initializes a new instance of the <see cref="ProjectionReader{T}" /> class.
        /// </summary>
        /// <param name="context">The context.</param>
        /// <param name="plan">The plan.</param>
        /// <param name="args">The arguments to fill the parameters of the (prepared) query</param>
        /// <exception cref="System.ArgumentNullException">context
        /// or
        /// cql
        /// or
        /// projector</exception>
        public TrackingReader(CqlContext context, QueryPlan plan, object[] args)
        {
            Debug.Assert(context != null, "Context may not be null");
            Debug.Assert(plan != null, "QueryPlan may not be null");

            _context = context;
            _plan = plan;
            _args = args;
        }

        #region IEnumerable<TEntity> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        public IEnumerator<TEntity> GetEnumerator()
        {
            var tracker = _context.ChangeTracker.GetTableChangeTracker<TEntity>();

            var connection = _context.Database.Connection;
            if (connection.State == ConnectionState.Closed)
                connection.Open();

            //log query
            _context.Database.LogQuery(_plan.Cql);

            var command = new CqlCommand(connection, _plan.Cql);

            if (_context.Database.CommandTimeout.HasValue)
                command.CommandTimeout = _context.Database.CommandTimeout.Value;

            if (_plan.Consistency.HasValue)
                command.Consistency = _plan.Consistency.Value;

            if (_plan.PageSize.HasValue)
                command.PageSize = _plan.PageSize.Value;

            if (_plan.VariableMap.Count > 0)
            {
                command.Prepare();
                for (int i = 0; i < _plan.VariableMap.Count; i++)
                {
                    int argumentIndex = _plan.VariableMap[i];
                    command.Parameters[i].Value = _args[argumentIndex];
                }
            }

            var projector = (Func<CqlDataReader, object[], TEntity>)_plan.Projector;

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var value = projector(reader, _args);
                    yield return tracker.GetOrAttach(value);
                }
            }
        }

        /// <summary>
        ///   Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns> An <see cref="T:System.Collections.IEnumerator" /> object that can be used to iterate through the collection. </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region IProjectionReader Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection, and returns the result as objects
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        public IEnumerable<object> AsObjectEnumerable()
        {
            return this.Select(elem => (object)elem);
        }

        #endregion
    }
}