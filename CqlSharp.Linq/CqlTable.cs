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
using System.Linq.Expressions;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A table in a Cassandra Keyspace (database)
    /// </summary>
    /// <typeparam name="T"> </typeparam>
    public class CqlTable<T> : IOrderedQueryable<T>, ICqlTable
    {
        private readonly CqlContext _context;
        private readonly Expression _expression;

        public CqlTable(CqlContext context)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            _context = context;
            _expression = Expression.Constant(this);
        }

        public CqlTable(CqlContext context, Expression expression)
        {
            if (context == null)
                throw new ArgumentNullException("context");

            if (expression == null)
                throw new ArgumentNullException("expression");

            _context = context;
            _expression = expression;
        }

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

        #region IOrderedQueryable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)Provider.Execute(_expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public Type ElementType
        {
            get { return typeof(T); }
        }

        public Expression Expression
        {
            get { return _expression; }
        }

        public IQueryProvider Provider
        {
            get { return _context; }
        }

        #endregion

        public override string ToString()
        {
            return string.Format("CqlTable<{0}>", ElementType.Name);
        }
    }
}