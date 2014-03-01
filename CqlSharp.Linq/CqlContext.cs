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

using CqlSharp.Linq.Expressions;
using CqlSharp.Linq.Translation;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   A representation of a Cql database (keyspace)
    /// </summary>
    public abstract class CqlContext : IQueryProvider, IDisposable
    {
        private string _connectionString;

        /// <summary>
        /// The list of tables known to this context
        /// </summary>
        private readonly ConcurrentDictionary<Type, ICqlTable> _tables;

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
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(CqlTable<>))
                {
                    //create new table object
                    var table = (ICqlTable)Activator.CreateInstance(propertyType, this);

                    //add it to the list of known tables
                    table = _tables.GetOrAdd(table.Type, table);

                    //set the property
                    property.SetValue(this, table);
                }
            }
        }

        /// <summary>
        /// Gets the table represented by the provided type.
        /// </summary>
        /// <typeparam name="T">type that represents the values in the table</typeparam>
        /// <returns>a CqlTable</returns>
        public CqlTable<T> GetTable<T>() where T : class, new()
        {
            return (CqlTable<T>)_tables.GetOrAdd(typeof(T), new CqlTable<T>(this));
        }

        public void SaveChanges()
        {
            throw new NotImplementedException();
        }

        private object Execute(Expression expression)
        {
            var result = ParseExpression(expression);

            //log the query
            if (Log != null)
                Log(result.Cql);

#if DEBUG
            //return default values of execution is to be skipped
            if (SkipExecute)
            {
                //return empty array
                if (result.ResultFunction == null)
                    return Array.CreateInstance(result.Projector.ReturnType, 0);

                //return default value or null
                return result.Projector.ReturnType.DefaultValue();
            }
#endif

            Delegate projector = result.Projector.Compile();

            var enm = (IProjectionReader)Activator.CreateInstance(
                typeof(ProjectionReader<>).MakeGenericType(result.Projector.ReturnType),
                BindingFlags.Instance | BindingFlags.Public, null,
                new object[] { this, result.Cql, projector },
                null
                                                );

            if (result.ResultFunction != null)
                return result.ResultFunction.Invoke(enm.AsObjectEnumerable());

            return enm;
        }

        internal class ParseResult
        {
            public string Cql { get; set; }
            public LambdaExpression Projector { get; set; }
            public ResultFunction ResultFunction { get; set; }
        }

        internal ParseResult ParseExpression(Expression expression)
        {
            Debug.WriteLine("Original Expression: " + expression);

            //evaluate all partial expressions (get rid of reference noise)
            var cleanedExpression = PartialEvaluator.Evaluate(expression, CanBeEvaluatedLocally);
            Debug.WriteLine("Cleaned Expression: " + cleanedExpression);

            //translate the expression to a cql expression and corresponding projection
            var translation = new ExpressionTranslator().Translate(cleanedExpression);

            //generate cql text
            var cql = new CqlTextBuilder().Build(translation.Select);
            Debug.WriteLine("Generated CQL: " + cql);

            //get a projection delegate
            var projector = new ProjectorBuilder().BuildProjector(translation.Projection);
            Debug.WriteLine("Generated Projector: " + projector);
            Debug.WriteLine("Result processor: " +
                            (translation.ResultFunction != null
                                 ? translation.ResultFunction.GetMethodInfo().ToString()
                                 : "<none>"));

            //return translation results
            return new ParseResult { Cql = cql, Projector = projector, ResultFunction = translation.ResultFunction };
        }

        private bool CanBeEvaluatedLocally(Expression expression)
        {
            var cex = expression as ConstantExpression;
            if (cex != null)
            {
                var query = cex.Value as IQueryable;
                if (query != null && query.Provider == this)
                    return false;
            }

            var mex = expression as MethodCallExpression;
            if (mex != null)
            {
                if (mex.Method.DeclaringType == typeof(CqlFunctions))
                    return false;
            }

            return expression.NodeType != ExpressionType.Parameter &&
                   expression.NodeType != ExpressionType.Lambda;
        }

        #region IQueryProvider implementation

        /// <summary>
        ///   Creates the query.
        /// </summary>
        /// <typeparam name="TElement"> The type of the element. </typeparam>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            return new CqlTable<TElement>(this, expression);
        }

        /// <summary>
        ///   Constructs an <see cref="T:System.Linq.IQueryable" /> object that can evaluate the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression"> An expression tree that represents a LINQ query. </param>
        /// <returns> An <see cref="T:System.Linq.IQueryable" /> that can evaluate the query represented by the specified expression tree. </returns>
        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return
                    (IQueryable)
                    Activator.CreateInstance(typeof(CqlTable<>).MakeGenericType(elementType),
                                             new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        /// <summary>
        ///   Executes the specified expression.
        /// </summary>
        /// <typeparam name="TResult"> The type of the result. </typeparam>
        /// <param name="expression"> The expression. </param>
        /// <returns> </returns>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
        {
            object result = Execute(expression);

            //convert known value types (long to int, etc) via their IConvertible interface
            if (result is IConvertible)
                return (TResult)Convert.ChangeType(result, typeof(TResult));

            //cast otherwise
            return (TResult)result;
        }

        /// <summary>
        ///   Executes the query represented by a specified expression tree.
        /// </summary>
        /// <param name="expression"> An expression tree that represents a LINQ query. </param>
        /// <returns> The value that results from executing the specified query. </returns>
        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        #endregion


    }
}