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
    using System.Linq;
    using NUnit.Framework;

    [TestFixture]
    public class SocketTests : AbstractZooKeeperTests
    {
        [Test]
        public void CanReadAndWriteOverASingleFrame()
        {
            SendBigByteArray(10000);
        }

        [Test]
        public void CanReadAndWriteOverTwoFrames()
        {
            SendBigByteArray(20000);
        }

        [Test]
        public void CanReadAndWriteOverManyFrames()
        {
            SendBigByteArray(100000);
        }

        private void SendBigByteArray(int bytes)
        {
            var b = new byte[bytes];
            foreach (var i in Enumerable.Range(0, bytes))
            {
                b[i] = Convert.ToByte(i % 255);
            }

            using (var client = CreateClient())
            {
                var node = "/" + Guid.NewGuid();
                client.Create(node, b, Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);

                var received = client.GetData(node, false, null);
                CollectionAssert.AreEqual(b, received);
            }
        }
    }
}
