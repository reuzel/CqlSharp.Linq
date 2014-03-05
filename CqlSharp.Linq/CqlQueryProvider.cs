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
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using CqlSharp.Linq.Expressions;
using CqlSharp.Linq.Translation;

namespace CqlSharp.Linq
{
    /// <summary>
    ///   Provider for the Cql Linq queries
    /// </summary>
    internal class CqlQueryProvider : IQueryProvider
    {
        private readonly CqlContext _cqlContext;

        internal CqlQueryProvider(CqlContext cqlContext)
        {
            _cqlContext = cqlContext;
        }

        private object Execute(Expression expression)
        {
            var result = ParseExpression(expression);

            //log the query
            if (_cqlContext.Log != null)
                _cqlContext.Log(result.Cql);

#if DEBUG
            //return default values of execution is to be skipped
            if (_cqlContext.SkipExecute)
            {
                //return empty array
                if (result.ResultFunction == null)
                    return Array.CreateInstance(result.Projector.ReturnType, 0);

                //return default value or null
                return result.Projector.ReturnType.DefaultValue();
            }
#endif

            Delegate projector = result.Projector.Compile();

            var enm = (IProjectionReader) Activator.CreateInstance(
                typeof (ProjectionReader<>).MakeGenericType(result.Projector.ReturnType),
                BindingFlags.Instance | BindingFlags.Public, null,
                new object[] {_cqlContext, result.Cql, projector},
                null
                                              );

            if (result.ResultFunction != null)
                return result.ResultFunction.Invoke(enm.AsObjectEnumerable());

            return enm;
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
            return new ParseResult {Cql = cql, Projector = projector, ResultFunction = translation.ResultFunction};
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
                if (mex.Method.DeclaringType == typeof (CqlFunctions))
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
            return new CqlTable<TElement>(_cqlContext, expression);
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
                    Activator.CreateInstance(typeof (CqlTable<>).MakeGenericType(elementType),
                                             new object[] {this, expression});
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
                return (TResult) Convert.ChangeType(result, typeof (TResult));

            //cast otherwise
            return (TResult) result;
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

        #region Nested type: ParseResult

        internal class ParseResult
        {
            public string Cql { get; set; }
            public LambdaExpression Projector { get; set; }
            public ResultFunction ResultFunction { get; set; }
        }

        #endregion
    }
}