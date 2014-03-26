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

using System.Linq.Expressions;

namespace CqlSharp.Linq.Expressions
{
    /// <summary>
    ///   Represents an Select statement and a additional mapping to C# constructs
    /// </summary>
    internal class ProjectionExpression : Expression
    {
        private readonly bool _canTrackChanges;
        private readonly Expression _projection;
        private readonly AggregateFunction _aggregator;
        private readonly SelectStatementExpression _select;
        private readonly int? _pageSize;
        private readonly CqlConsistency? _consistency;

        public ProjectionExpression(SelectStatementExpression @select, Expression projection, AggregateFunction aggregator, bool canTrackChanges, CqlConsistency? consistency, int? pageSize)
        {
            _select = @select;
            _projection = projection;
            _canTrackChanges = canTrackChanges;
            _consistency = consistency;
            _pageSize = pageSize;
            _aggregator = aggregator;
        }

        public SelectStatementExpression Select
        {
            get { return _select; }
        }

        public Expression Projection
        {
            get { return _projection; }
        }

        public override ExpressionType NodeType
        {
            get { return (ExpressionType)CqlExpressionType.Projection; }
        }

        public AggregateFunction Aggregator
        {
            get { return _aggregator; }
        }

        public bool CanTrackChanges
        {
            get { return _canTrackChanges; }
        }

        public CqlConsistency? Consistency
        {
            get { return _consistency; }
        }

        public int? PageSize
        {
            get { return _pageSize; }
        }

        protected override Expression Accept(ExpressionVisitor visitor)
        {
            var type = visitor as CqlExpressionVisitor;

            if (type != null)
            {
                return type.VisitProjection(this);
            }

            return base.Accept(visitor);
        }

        protected override Expression VisitChildren(ExpressionVisitor visitor)
        {
            var selector = (SelectStatementExpression)visitor.Visit(_select);
            var projector = visitor.Visit(_projection);

            if (selector != _select || projector != _projection)
                return new ProjectionExpression(selector, projector, _aggregator, _canTrackChanges, _consistency, _pageSize);

            return this;
        }
    }
}