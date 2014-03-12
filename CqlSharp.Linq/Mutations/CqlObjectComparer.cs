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
using System.Linq;
using CqlSharp.Serialization;

namespace CqlSharp.Linq.Mutations
{
    public class CqlObjectComparer<TObject> : IEqualityComparer<TObject>, IEqualityComparer
    {
        /// <summary>
        ///   The singleton instance
        /// </summary>
        public static readonly CqlObjectComparer<TObject> Instance = new CqlObjectComparer<TObject>();

        #region IEqualityComparer Members

        /// <summary>
        ///   Determines whether the specified objects are equal.
        /// </summary>
        /// <returns> true if the specified objects are equal; otherwise, false. </returns>
        /// <param name="x"> The first object to compare. </param>
        /// <param name="y"> The second object to compare. </param>
        /// <exception cref="T:System.ArgumentException">
        ///   <paramref name="x" />
        ///   and
        ///   <paramref name="y" />
        ///   are of different types and neither one can handle comparisons with the other.</exception>
        public new bool Equals(object x, object y)
        {
            return Equals((TObject) x, (TObject) y);
        }

        /// <summary>
        ///   Returns a hash code for the specified object.
        /// </summary>
        /// <returns> A hash code for the specified object. </returns>
        /// <param name="obj"> The <see cref="T:System.Object" /> for which a hash code is to be returned. </param>
        /// <exception cref="T:System.ArgumentNullException">The type of
        ///   <paramref name="obj" />
        ///   is a reference type and
        ///   <paramref name="obj" />
        ///   is null.</exception>
        public int GetHashCode(object obj)
        {
            return GetHashCode((TObject) obj);
        }

        #endregion

        #region IEqualityComparer<TObject> Members

        /// <summary>
        ///   Determines whether the specified objects are equal.
        /// </summary>
        /// <returns> true if the specified objects are equal; otherwise, false. </returns>
        /// <param name="x"> The first object of type <paramref name="T" /> to compare. </param>
        /// <param name="y"> The second object of type <paramref name="T" /> to compare. </param>
        public bool Equals(TObject x, TObject y)
        {
            var accessor = ObjectAccessor<TObject>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (column.ReadFunction != null)
                {
                    var valX = column.ReadFunction(x);
                    var valY = column.ReadFunction(y);

                    if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                    {
                        if (!TypeSystem.SequenceEqual((IEnumerable) valX, (IEnumerable) valY))
                            return false;
                    }
                    else if (!Object.Equals(valX, valY))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        ///   Returns a hash code for the specified object.
        /// </summary>
        /// <returns> A hash code for the specified object. </returns>
        /// <param name="obj"> The <see cref="T:System.Object" /> for which a hash code is to be returned. </param>
        /// <exception cref="T:System.ArgumentNullException">The type of
        ///   <paramref name="obj" />
        ///   is a reference type and
        ///   <paramref name="obj" />
        ///   is null.</exception>
        public int GetHashCode(TObject obj)
        {
            if (obj == null)
                throw new ArgumentNullException("obj");

            int hashCode = 1;

            var accessor = ObjectAccessor<TObject>.Instance;
            foreach (var column in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                var value = column.ReadFunction(obj);
                hashCode = hashCode*31 + (value == null ? 0 : value.GetHashCode());
            }

            return hashCode;
        }

        #endregion
    }
}