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
using System.Linq;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Executes a CQL query and transforms the results into object using a projector
    /// </summary>
    /// <typeparam name="TElement"> type of elements returned from a query </typeparam>
    internal class ProjectionReader<TElement> : IEnumerable<TElement>, IProjectionReader
    {
        private readonly CqlContext _context;
        private readonly string _cql;
        private readonly Func<CqlDataReader, TElement> _projector;

        /// <summary>
        ///   Initializes a new instance of the <see cref="ProjectionReader{T}" /> class.
        /// </summary>
        /// <param name="context"> The context. </param>
        /// <param name="cql"> The CQL. </param>
        /// <param name="projector"> The projector. </param>
        /// <exception cref="System.ArgumentNullException">context
        ///   or
        ///   cql
        ///   or
        ///   projector</exception>
        public ProjectionReader(CqlContext context, string cql, Func<CqlDataReader, TElement> projector)
        {
            if (context == null) throw new ArgumentNullException("context");
            if (cql == null) throw new ArgumentNullException("cql");
            if (projector == null) throw new ArgumentNullException("projector");

            _context = context;
            _cql = cql;
            _projector = projector;
        }

        #region IEnumerable<TElement> Members

        /// <summary>
        ///   Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns> A <see cref="T:System.Collections.Generic.IEnumerator`1" /> that can be used to iterate through the collection. </returns>
        public IEnumerator<TElement> GetEnumerator()
        {
            var connection = _context.Database.Connection;

            if (connection.State == ConnectionState.Closed)
                connection.Open();

            //log query
            _context.Database.LogQuery(_cql);

            var command = new CqlCommand(connection, _cql);

            if (_context.Database.CommandTimeout.HasValue)
                command.CommandTimeout = _context.Database.CommandTimeout.Value;

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    yield return _projector(reader);
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
            return this.Select(elem => (object) elem);
        }

        #endregion
    }
}