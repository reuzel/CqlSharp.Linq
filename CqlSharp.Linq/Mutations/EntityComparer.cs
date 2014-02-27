using CqlSharp.Serialization;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    /// Compares two entities for equality.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    internal class EntityComparer<TEntity> : IEqualityComparer<TEntity>
    {
        /// <summary>
        /// Determines whether the specified objects are equal.
        /// </summary>
        /// <returns>
        /// true if the specified objects are equal; otherwise, false.
        /// </returns>
        /// <param name="x">The first object of type <paramref name="TEntity"/> to compare.</param><param name="y">The second object of type <paramref name="T"/> to compare.</param>
        public bool Equals(TEntity x, TEntity y)
        {
            if (ReferenceEquals(x, y)) return true;

            var accessor = ObjectAccessor<TEntity>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (column.ReadFunction != null)
                {
                    var valX = column.ReadFunction(x);
                    var valY = column.ReadFunction(y);

                    if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                    {
                        if (!TypeSystem.SequenceEqual((IEnumerable)valX, (IEnumerable)valY))
                            return false;
                    }
                    else if (!Equals(valX, valY))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Returns a hash code for the specified object.
        /// </summary>
        /// <returns>
        /// A hash code for the specified object.
        /// </returns>
        /// <param name="obj">The <see cref="T:System.Object"/> for which a hash code is to be returned.</param><exception cref="T:System.ArgumentNullException">The type of <paramref name="obj"/> is a reference type and <paramref name="obj"/> is null.</exception>
        public int GetHashCode(TEntity obj)
        {
            int hashCode = 1;

            var accessor = ObjectAccessor<TEntity>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                var value = column.ReadFunction(obj);
                hashCode = hashCode * 31 + (value == null ? 0 : obj.GetHashCode());
            }

            return hashCode;
        }
    }
}