/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
namespace ZooKeeperNet.Recipes
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;

    public class SortedSet<T> : ICollection<T> where T : IComparable<T>
    {
        private readonly SortedDictionary<T, object> backing = new SortedDictionary<T, object>(ComparableComparer.Instance);

        private class ComparableComparer : IComparer<T>
        {
            internal static readonly ComparableComparer Instance = new ComparableComparer();

            public int Compare(T x, T y)
            {
                return x.CompareTo(y);
            }
        }

        public void Add(T element)
        {
            backing[element] = new object();
        }

        public void Clear()
        {
            backing.Clear();
        }

        public bool Contains(T item)
        {
            return backing.ContainsKey(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            var keys = backing.Keys.ToArray();
            Array.Copy(keys, array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return backing.Remove(item);
        }

        public int Count
        {
            get { return backing.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (var key in backing.Keys)
                yield return key;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public SortedSet<T> HeadSet(T element)
        {
            var headSet = new SortedSet<T>();
            foreach (var key in backing.Keys.TakeWhile(key => !key.Equals(element)))
                headSet.Add(key);
            
            return headSet;
        }
    }
}