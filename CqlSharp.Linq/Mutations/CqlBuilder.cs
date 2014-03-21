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
using System.Linq;
using System.Text;
using CqlSharp.Serialization;

namespace CqlSharp.Linq.Mutations
{
    internal static class CqlBuilder<TEntity> where TEntity : class, new()
    {
        private static readonly ObjectAccessor<TEntity> Accessor = ObjectAccessor<TEntity>.Instance;

        /// <summary>
        ///   Gets the reload CQL statement.
        /// </summary>
        /// <returns> </returns>
        public static string GetSelectQuery(CqlTable<TEntity> table, EntityKey<TEntity> key)
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
            sb.Append(table.Name);
            sb.Append("\" WHERE");

            firstColumn = true;
            foreach (var keyColumn in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (!firstColumn)
                    sb.Append(" AND ");
                sb.Append(" \"");
                sb.Append(keyColumn.Name);
                sb.Append("\"=");
                var value = keyColumn.ReadFunction(key.Values);
                sb.Append(TypeSystem.ToStringValue(value, keyColumn.CqlType));
                firstColumn = false;
            }

            return sb.ToString();
        }

        /// <summary>
        ///   Builds the DML query.
        /// </summary>
        /// <param name="trackedItem"> The tracked item. </param>
        /// <returns> </returns>
        /// <exception cref="System.InvalidOperationException"></exception>
        /// <exception cref="System.NotImplementedException">InsertOrUpdate is not yet implemented
        ///   or
        ///   PossibleUpdate is not yet implemented</exception>
        public static string BuildDmlQuery(TrackedEntity<TEntity> trackedItem)
        {
            switch (trackedItem.State)
            {
                case EntityState.Deleted:
                    return BuildDeleteStatement(trackedItem);
                case EntityState.Added:
                    return BuildInsertStatement(trackedItem);
                case EntityState.Modified:
                    return BuildUpdateStatement(trackedItem);
                case EntityState.Unchanged:
                    return string.Empty;
                default:
                    throw new InvalidOperationException();
            }
        }

        #region Delete functions

        private static string BuildDeleteStatement(TrackedEntity<TEntity> trackedItem)
        {
            var deleteSb = new StringBuilder();
            deleteSb.Append("DELETE FROM \"");
            deleteSb.Append(trackedItem.Table.Name.Replace("\"", "\"\""));
            deleteSb.Append("\" WHERE ");
            TranslatePrimaryConditions(deleteSb, trackedItem);
            deleteSb.Append(";");

            return deleteSb.ToString();
        }

        #endregion

        #region Update functions

        private static string BuildUpdateStatement(TrackedEntity<TEntity> trackedItem)
        {
            var updateSb = new StringBuilder();
            updateSb.Append("UPDATE \"");
            updateSb.Append(trackedItem.Table.Name.Replace("\"", "\"\""));
            updateSb.Append("\" SET ");
            TranslateUpdationIdValPairs(updateSb, trackedItem);
            updateSb.Append(" WHERE ");
            TranslatePrimaryConditions(updateSb, trackedItem);
            updateSb.Append(";");

            return updateSb.ToString();
        }

        private static void TranslateUpdationIdValPairs(StringBuilder builder, TrackedEntity<TEntity> trackedEntity)
        {
            bool first = true;
            foreach (CqlColumnInfo<TEntity> column in trackedEntity.ChangedColumns)
            {
                if (!first)
                    builder.Append(", ");

                builder.Append("\"");
                builder.Append(column.Name.Replace("\"", "\"\""));
                builder.Append("\"=");

                var value = column.ReadFunction(trackedEntity.Entity);
                builder.Append(TypeSystem.ToStringValue(value, column.CqlType));

                first = false;
            }
        }

        private static void TranslatePrimaryConditions(StringBuilder builder, TrackedEntity<TEntity> trackedEntity)
        {
            bool first = true;
            foreach (var keyColumn in Accessor.PartitionKeys.Concat(Accessor.ClusteringKeys))
            {
                if (!first)
                    builder.Append(" AND ");

                builder.Append("\"");
                builder.Append(keyColumn.Name.Replace("\"", "\"\""));
                builder.Append("\"=");
                var value = keyColumn.ReadFunction(trackedEntity.Entity);
                builder.Append(TypeSystem.ToStringValue(value, keyColumn.CqlType));

                first = false;
            }
        }

        #endregion

        #region Insert functions

        private static string BuildInsertStatement(TrackedEntity<TEntity> trackedItem)
        {
            var insertSb = new StringBuilder();
            insertSb.Append("INSERT INTO \"");
            insertSb.Append(trackedItem.Table.Name.Replace("\"", "\"\""));
            insertSb.Append("\" (");
            TranslateInsertionIds(insertSb, trackedItem);
            insertSb.Append(")");
            insertSb.Append(" VALUES ");
            insertSb.Append("(");
            TranslateInsertionValues(insertSb, trackedItem);
            insertSb.Append(");");

            return insertSb.ToString();
        }

        private static void TranslateInsertionIds(StringBuilder builder, TrackedEntity<TEntity> trackedEntity)
        {
            bool first = true;
            foreach (var column in Accessor.Columns)
            {
                //skip null values
                if (column.ReadFunction(trackedEntity.Entity) == null)
                    continue;

                if (!first)
                    builder.Append(", ");

                builder.Append("\"");
                builder.Append(column.Name.Replace("\"", "\"\""));
                builder.Append("\"");

                first = false;
            }
        }

        private static void TranslateInsertionValues(StringBuilder builder, TrackedEntity<TEntity> trackedEntity)
        {
            bool first = true;
            foreach (var column in Accessor.Columns)
            {
                //skip null values
                var value = column.ReadFunction(trackedEntity.Entity);
                if (value == null)
                    continue;

                //add ',' if not first
                if (!first)
                    builder.Append(", ");

                //write value
                builder.Append(TypeSystem.ToStringValue(value, column.CqlType));

                first = false;
            }
        }

        #endregion
    }
}