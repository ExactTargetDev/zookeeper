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
    using log4net;

    public class ZNodeName : IComparable<ZNodeName>, IEquatable<ZNodeName>
    {
        private static readonly ILog LOG = LogManager.GetLogger(typeof(ZNodeName));        
        private readonly int sequence;

        public ZNodeName(string name)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentException("name");

            Name = name;
            Prefix = name;

            int idx = name.LastIndexOf('-');
            if (idx >= 0)
            {
                Prefix = name.Substring(0, idx);
                if (!Int32.TryParse(name.Substring(idx + 1), out sequence))
                {
                    LOG.Info("Could not parse number for " + idx);
                    sequence = -1;
                }
            }
        }

        public string Name { get; set; }
        protected string Prefix { get; set; }
        
        protected int NodeName
        {
            get
            {
                return sequence;
            }
        }

        public override string ToString()
        {
            return Name;
        }

        public bool Equals(ZNodeName other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(other.Name, Name);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof (ZNodeName)) return false;
            return Equals((ZNodeName) obj);
        }

        public override int GetHashCode()
        {
            return (Name != null ? Name.GetHashCode() : 0);
        }

        public int CompareTo(ZNodeName other)
        {
            int answer = Prefix.CompareTo(other.Prefix);
            if (answer == 0)
            {
                int s1 = this.sequence;
                int s2 = other.sequence;
                if (s1 == -1 && s2 == -1)
                {
                    return Name.CompareTo(other.Name);
                }
                answer = s1 == -1 ? 1 : s2 == -1 ? -1 : s1 - s2;
            }
            return answer;
        }
    }
}
