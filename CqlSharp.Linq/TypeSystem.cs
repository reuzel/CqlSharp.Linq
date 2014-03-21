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
using System.Globalization;
using System.Numerics;
using CqlSharp.Serialization;

namespace CqlSharp.Linq
{
    internal static class TypeSystem
    {
        internal static object DefaultValue(this Type type)
        {
            return type.IsValueType ? Activator.CreateInstance(type) : null;
        }

        internal static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null) return seqType;
            return ienum.GetGenericArguments()[0];
        }

        public static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof (string))
                return null;
            if (seqType.IsArray)
                return typeof (IEnumerable<>).MakeGenericType(seqType.GetElementType());
            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof (IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }
            Type[] ifaces = seqType.GetInterfaces();
            if (ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null) return ienum;
                }
            }
            if (seqType.BaseType != null && seqType.BaseType != typeof (object))
            {
                return FindIEnumerable(seqType.BaseType);
            }
            return null;
        }

        public static bool Implements(this Type type, Type iface)
        {
            if (type == iface)
                return true;

            bool findGeneric = iface.IsGenericTypeDefinition;

            if (findGeneric && type.IsGenericType && type.GetGenericTypeDefinition() == iface)
                return true;

            Type[] interfaces = type.GetInterfaces();

            foreach (var i in interfaces)
            {
                if (findGeneric && i.IsGenericType)
                {
                    if (i.IsGenericTypeDefinition && iface == i)
                        return true;

                    if (iface == i.GetGenericTypeDefinition())
                        return true;
                }

                if (!findGeneric && !i.IsGenericType)
                {
                    if (iface == i)
                        return true;
                }
            }

            return false;
        }

        public static bool SequenceEqual(IEnumerable first, IEnumerable second)
        {
            if (first == null && second == null)
                return true;

            if (first == null || second == null)
                return false;

            var enumerator1 = first.GetEnumerator();
            var enumerator2 = second.GetEnumerator();

            while (enumerator1.MoveNext())
            {
                if (!enumerator2.MoveNext() || !Equals(enumerator1.Current, enumerator2.Current))
                    return false;
            }

            if (enumerator2.MoveNext())
                return false;

            return true;
        }


        /// <summary>
        ///   Clones the specified entity.
        /// </summary>
        /// <param name="entity"> The entity. </param>
        /// <param name="keyOnly"> clones the key values only </param>
        /// <returns> </returns>
        internal static TEntity Clone<TEntity>(this TEntity entity, bool keyOnly = false)
        {
            var clone = Activator.CreateInstance<TEntity>();
            entity.CopyTo(clone, keyOnly);
            return clone;
        }

        /// <summary>
        ///   Copies the values from one entity to another
        /// </summary>
        /// <param name="source"> The entity. </param>
        /// <param name="destination"> the entity </param>
        /// <param name="keyOnly"> clones the key values only </param>
        /// <returns> </returns>
        internal static void CopyTo<TEntity>(this TEntity source, TEntity destination, bool keyOnly = false)
        {
            if (source == null)
                throw new ArgumentNullException("source");

            if (destination == null)
                throw new ArgumentNullException("destination");

            foreach (CqlColumnInfo<TEntity> column in ObjectAccessor<TEntity>.Instance.Columns)
            {
                //make sure value can be cloned
                if (column.ReadFunction == null || column.WriteFunction == null) continue;

                //skip if column is not a key column, and only keys need to be cloned
                if (keyOnly && !column.IsPartitionKey && !column.IsClusteringKey)
                    continue;

                //get the value
                var value = column.ReadFunction(source);

                //mandate that all key values are set
                if (value == null && (column.IsPartitionKey || column.IsClusteringKey))
                    throw new CqlLinqException("Can not track an object who's key values are not completely set");

                //if the column is a collection type, clone the collection
                if (column.CqlType == CqlType.List || column.CqlType == CqlType.Map || column.CqlType == CqlType.Set)
                {
                    value = Activator.CreateInstance(column.Type, value);
                }

                //copy the value into the clone
                column.WriteFunction(destination, value);
            }
        }

        /// <summary>
        ///   Translates the object to its Cql string representation.
        /// </summary>
        /// <param name="value"> The value. </param>
        /// <param name="type"> The type. </param>
        /// <returns> </returns>
        /// <exception cref="CqlLinqException">Unable to translate term to a string representation</exception>
        public static string ToStringValue(object value, CqlType type)
        {
            switch (type)
            {
                case CqlType.Text:
                case CqlType.Varchar:
                case CqlType.Ascii:
                    var str = (string) value;
                    return "'" + str.Replace("'", "''") + "'";

                case CqlType.Boolean:
                    return ((bool) value) ? "true" : "false";

                case CqlType.Decimal:
                case CqlType.Double:
                case CqlType.Float:
                    var culture = CultureInfo.InvariantCulture;
                    return string.Format(culture, "{0:E}", value);

                case CqlType.Counter:
                case CqlType.Bigint:
                case CqlType.Int:
                    return string.Format("{0:D}", value);

                case CqlType.Timeuuid:
                case CqlType.Uuid:
                    return ((Guid) value).ToString("D");

                case CqlType.Varint:
                    return ((BigInteger) value).ToString("D");

                case CqlType.Timestamp:
                    long timestamp = ((DateTime) value).ToTimestamp();
                    return string.Format("{0:D}", timestamp);

                case CqlType.Blob:
                    return ((byte[]) value).ToHex("0x");

                default:
                    throw new CqlLinqException("Unable to translate term to a string representation");
            }
        }
    }
}