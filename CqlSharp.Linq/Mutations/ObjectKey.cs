﻿// CqlSharp.Linq - CqlSharp.Linq
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
using System.Linq;
using CqlSharp.Serialization;

namespace CqlSharp.Linq.Mutations
{
    /// <summary>
    ///   Defines a key from a table entry
    /// </summary>
    internal struct ObjectKey
    {
        /// <summary>
        ///   The comparer that can compare Cql objects based on their key values
        /// </summary>
        private readonly IEqualityComparer _keyComparer;

        /// <summary>
        ///   The object from which the key is derived
        /// </summary>
        private readonly Object _object;

        /// <summary>
        ///   Initializes a new instance of the <see cref="ObjectKey" /> struct.
        /// </summary>
        /// <param name="obj"> The object. </param>
        /// <param name="keyComparer"> </param>
        private ObjectKey(object obj, IEqualityComparer keyComparer)
        {
            _object = obj;
            _keyComparer = keyComparer;
        }

        /// <summary>
        ///   Creates an ObjectKey from the specified entity.
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <param name="entity"> The entity. </param>
        /// <returns> </returns>
        public static ObjectKey Create<TEntity>(TEntity entity)
        {
            return new ObjectKey(entity, CqlObjectComparer<TEntity>.Instance);
        }

        /// <summary>
        ///   Creates an ObjectKey from the specified key values.
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <param name="keyValues"> The key values. </param>
        /// <returns> </returns>
        /// <exception cref="System.ArgumentException">Not all required key values are provided
        ///   or
        ///   the types of the keyValues do not match the required types for the entity keys</exception>
        public static ObjectKey Create<TEntity>(params object[] keyValues)
        {
            var accessor = ObjectAccessor<TEntity>.Instance;
            var keyObject = Activator.CreateInstance<TEntity>();

            int index = 0;
            foreach (var keyColumn in accessor.PartitionKeys.Concat(accessor.ClusteringKeys))
            {
                if (index >= keyValues.Length)
                    throw new ArgumentException("Not all required key values are provided", "keyValues");

                if (keyValues[index].GetType() != keyColumn.Type)
                    throw new ArgumentException(
                        String.Format(
                            "The key value at index {0} has type {1}, which does not match the required key type {2} ",
                            index, keyValues[index].GetType().FullName, keyColumn.Type.FullName), "keyValues");

                keyColumn.WriteFunction(keyObject, keyValues[index++]);
            }

            return Create(keyObject);
        }

        /// <summary>
        ///   Gets the key values.
        /// </summary>
        /// <returns> </returns>
        public Object GetKeyValues()
        {
            return _object;
        }

        /// <summary>
        ///   Gets the key values.
        /// </summary>
        /// <typeparam name="TEntity"> The type of the entity. </typeparam>
        /// <returns> </returns>
        public TEntity GetKeyValues<TEntity>()
        {
            return (TEntity) _object;
        }

        /// <summary>
        ///   Determines whether the specified <see cref="System.Object" />, is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="System.Object" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (!(obj is ObjectKey)) return false;
            return Equals((ObjectKey) obj);
        }

        /// <summary>
        ///   Determines whether the specified <see cref="ObjectKey" />, is equal to this instance.
        /// </summary>
        /// <param name="obj"> The <see cref="ObjectKey" /> to compare with this instance. </param>
        /// <returns> <c>true</c> if the specified <see cref="ObjectKey" /> is equal to this instance; otherwise, <c>false</c> . </returns>
        public bool Equals(ObjectKey obj)
        {
            //true if both are default object keys
            if (_object == null && obj._object == null) return true;

            //false if only one is object key
            if (_object == null || obj._object == null) return false;

            //false if the key objects do not match type
            if (obj._object.GetType() != _object.GetType()) return false;

            //check data with the comparer
            return _keyComparer.Equals(_object, obj._object);
        }

        /// <summary>
        ///   Returns a hash code for this instance.
        /// </summary>
        /// <returns> A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table. </returns>
        public override int GetHashCode()
        {
            return _keyComparer.GetHashCode(_object);
        }

        public static bool operator ==(ObjectKey first, ObjectKey second)
        {
            return first.Equals(second);
        }

        public static bool operator !=(ObjectKey first, ObjectKey second)
        {
            return !(first == second);
        }
    }
}