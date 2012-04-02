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
namespace ZooKeeperNet.Tests
{
    using System;
    using System.Threading;

    public class CountDownLatch
    {
        private readonly ManualResetEvent reset;
        private readonly int occurences;
        private int count;
        private DateTime start;
        private TimeSpan remaining;

        public CountDownLatch(int occurences)
        {
            this.occurences = occurences;
            reset = new ManualResetEvent(false);
        }

        public bool Await(TimeSpan wait)
        {
            start = DateTime.Now;
            remaining = wait;
            while (count < occurences)
            {
                if (!reset.WaitOne(remaining))
                    return false;
            }
            return true;
        }

        public void CountDown()
        {
            remaining = DateTime.Now - start;
            Interlocked.Increment(ref count);
            reset.Set();
        }

        public int Count
        {
            get
            {
                return count;
            }
        }
    }
}