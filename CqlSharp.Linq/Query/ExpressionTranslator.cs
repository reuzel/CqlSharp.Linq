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
using System.Linq;
using System.Linq.Expressions;
using CqlSharp.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Translates a Linq Expression tree into a Cql expression tree
    /// </summary>
    internal class ExpressionTranslator : CqlExpressionVisitor
    {
        public ProjectionExpression Translate(Expression expression)
        {
            return (ProjectionExpression) Visit(expression);
        }

        protected override Expression VisitConstant(ConstantExpression constant)
        {
            var table = constant.Value as ICqlTable;
            if (table != null)
            {
                return CreateTableProjection(table);
            }

            return base.VisitConstant(constant);
        }

        /// <summary>
        ///   Creates the table projection.
        /// </summary>
        /// <param name="table"> The table. </param>
        /// <returns> </returns>
        private static Expression CreateTableProjection(ICqlTable table)
        {
            var enumType = typeof (IEnumerable<>).MakeGenericType(table.EntityType);

            var selectors = new List<SelectorExpression>();
            var bindings = new List<MemberBinding>();
            foreach (var column in table.Columns)
            {
                var identifierType = column.Type;

                var identifierName = column.Name;

                var identifier = new SelectorExpression(identifierName, identifierType);

                selectors.Add(identifier);

                bindings.Add(Expression.Bind(column.MemberInfo, identifier));
            }

            var selectClause = new SelectClauseExpression(selectors, false);
            var selectStmt = new SelectStatementExpression(enumType, selectClause, table.Name, null, null, null, false);

            var projection = Expression.MemberInit(Expression.New(table.EntityType), bindings);

            return new ProjectionExpression(selectStmt, projection, true, null);
        }

        protected override Expression VisitMethodCall(MethodCallExpression call)
        {
            if (call.Method.DeclaringType == typeof (CqlQueryable))
            {
                switch (call.Method.Name)
                {
                    case "AllowFiltering":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       source.Select.Limit,
                                                                       true);

                            return new ProjectionExpression(select, source.Projection, source.CanTrackChanges,
                                                            source.Aggregator);
                        }
                }
            }
            else if (call.Method.DeclaringType == typeof (Queryable))
            {
                switch (call.Method.Name)
                {
                    case "Select":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);
                            return new SelectBuilder().UpdateSelect(source, call.Arguments[1]);
                        }

                    case "Where":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException(
                                    "A Where statement may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the Take after the Where statement.");

                            return new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                        }

                    case "Distinct":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //make sure limit is not set yet (as otherwise the semantics of the query cannot be supported)
                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException("Any Take operation most occur after Distinct");

                            //set distinct on the select clause
                            var selectClause = new SelectClauseExpression(source.Select.SelectClause.Selectors, true);

                            //update select
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       selectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       source.Select.Limit,
                                                                       source.Select.AllowFiltering);

                            //update projection
                            return new ProjectionExpression(select, source.Projection, false, source.Aggregator);
                        }

                    case "Take":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //get take
                            var take = (int) ((ConstantExpression) call.Arguments[1]).Value;

                            //use minimum of takes...
                            take = source.Select.Limit.HasValue ? Math.Min(source.Select.Limit.Value, take) : take;

                            //add limit to return given amount of results
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       take,
                                                                       source.Select.AllowFiltering);

                            return new ProjectionExpression(select, source.Projection, source.CanTrackChanges,
                                                            source.Aggregator);
                        }

                    case "First":
                    case "FirstOrDefault":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A First statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       1,
                                                                       source.Select.AllowFiltering);

                            //use Enumerable logic for processing result set
                            AggregateFunction processor = call.Method.Name.Equals("First")
                                                           ? Enumerable.First
                                                           : (AggregateFunction) Enumerable.FirstOrDefault;

                            return new ProjectionExpression(select, source.Projection, source.CanTrackChanges, processor);
                        }

                    case "Single":
                    case "SingleOrDefault":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Single statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //set the limit to min of current limit or 2
                            int limit = source.Select.Limit.HasValue ? Math.Min(source.Select.Limit.Value, 2) : 2;

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       limit,
                                                                       source.Select.AllowFiltering);

                            //use Enumerable logic for processing result set
                            AggregateFunction processor = call.Method.Name.Equals("Single")
                                                           ? Enumerable.Single
                                                           : (AggregateFunction) Enumerable.SingleOrDefault;


                            return new ProjectionExpression(select, source.Projection, source.CanTrackChanges, processor);
                        }

                    case "Any":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "An Any statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //add limit to return single result
                            var select = new SelectStatementExpression(source.Select.Type,
                                                                       source.Select.SelectClause,
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       source.Select.OrderBy,
                                                                       1,
                                                                       source.Select.AllowFiltering);

                            return new ProjectionExpression(select, source.Projection, false, enm => enm.Any());
                        }

                    case "Count":
                    case "LongCount":
                        {
                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            //count and distinct do not go together in CQL
                            if (source.Select.SelectClause.Distinct)
                                throw new CqlLinqException("Count cannot be combined with Distinct in CQL");

                            //if first contains a predicate, include it in the where clause...
                            if (call.Arguments.Count > 1)
                            {
                                if (source.Select.Limit.HasValue)
                                    throw new CqlLinqException(
                                        "A Count statement with a condition may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the condition into a Where clause executed before the Take.");

                                source = new WhereBuilder().BuildWhere(source, call.Arguments[1]);
                            }

                            //remove the select clause and replace with count(*)
                            var select = new SelectStatementExpression(typeof (long),
                                                                       new SelectClauseExpression(true),
                                                                       source.Select.TableName,
                                                                       source.Select.WhereClause,
                                                                       null,
                                                                       source.Select.Limit,
                                                                       source.Select.AllowFiltering);

                            return new ProjectionExpression(select, new SelectorExpression("count", typeof (long)),
                                                            false, Enumerable.Single);
                        }

                    case "OrderBy":
                    case "OrderByDescending":
                    case "ThenBy":
                    case "ThenByDescending":
                        {
                            if (call.Arguments.Count > 2)
                                throw new CqlLinqException(
                                    "Custom IComparer implementations are not supported in ordering expressions.");

                            var source = (ProjectionExpression) Visit(call.Arguments[0]);

                            if (source.Select.Limit.HasValue)
                                throw new CqlLinqException(
                                    "An OrderBy or ThenBy statement may not follow a query that contains a limit on returned results. If you use Take(int) consider moving the Take after the ordering statement.");

                            bool ascending = call.Method.Name.Equals("OrderBy") ||
                                             call.Method.Name.Equals("ThenBy");

                            return new OrderBuilder().UpdateOrder(source, call.Arguments[1], ascending);
                        }

                    default:
                        throw new CqlLinqException(string.Format("Method {0} is not supported", call.Method));
                }
            }

            return call;
        }
    }
}