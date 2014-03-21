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
using System.Reflection;
using CqlSharp.Linq.Expressions;

namespace CqlSharp.Linq.Query
{
    /// <summary>
    ///   Converts an expression with selector expressions to a lambda expression that takes a datareader as input.
    /// </summary>
    internal class ProjectorBuilder : CqlExpressionVisitor
    {
        private static readonly ConstructorInfo TokenConstructor =
            typeof (CqlToken).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null,
                                             CallingConventions.HasThis,
                                             new[] {typeof (object)}, null);

        private static readonly PropertyInfo Indexer = typeof (CqlDataReader).GetProperty("Item",
                                                                                          new[] {typeof (int)});

        private ParameterExpression _reader;

        public LambdaExpression BuildProjector(Expression expression)
        {
            _reader = Expression.Parameter(typeof (CqlDataReader), "cqlDataReader");
            Expression expr = Visit(expression);
            return Expression.Lambda(expr, _reader);
        }

        public override Expression VisitSelector(SelectorExpression selector)
        {
            var value = Expression.MakeIndex(_reader, Indexer, new[] {Expression.Constant(selector.Ordinal)});

            //check if it is a token (of which we don't know it's type)
            if (selector.Type == typeof (CqlToken))
                return Expression.New(TokenConstructor, value);

            //check if it is a value type (and change null values to corresponding default values)
            if (selector.Type.IsValueType)
            {
                return Expression.Condition(
                    Expression.Equal(value, Expression.Constant(null)),
                    Expression.Default(selector.Type),
                    Expression.Convert(value, selector.Type));
            }

            //any other class value
            return Expression.Convert(value, selector.Type);
        }
    }
}