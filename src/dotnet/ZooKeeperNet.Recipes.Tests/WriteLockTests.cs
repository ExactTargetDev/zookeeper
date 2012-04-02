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
namespace ZooKeeperNetRecipes.Tests
{
    using System;
    using System.Threading;
    using log4net;
    using NUnit.Framework;
    using ZooKeeperNet;
    using ZooKeeperNet.Recipes;
    using ZooKeeperNet.Tests;

    [TestFixture]
    public class WriteLockTests : AbstractZooKeeperTests
    {
        private static readonly ILog LOG = LogManager.GetLogger(typeof(WriteLockTests));

        protected int sessionTimeout = 10 * 1000;
        protected String dir = "/" + Guid.NewGuid();
        protected WriteLock[] nodes;

        [Test]
        public void testRun()
        {
            runTest(3);
        }

        protected void runTest(int count)
        {
            ManualResetEvent[] waitHandles = new ManualResetEvent[count];
            nodes = new WriteLock[count];
            for (int i = 0; i < count; i++)
            {
                waitHandles[i] = new ManualResetEvent(true);
                ZooKeeper keeper = CreateClient();
                WriteLock leader = new WriteLock(keeper, dir, null);
                leader.LockAcquired += () => waitHandles[i].Set();
                nodes[i] = leader;
                leader.Lock();
            }

            // lets wait for any previous leaders to die and one of our new
            // nodes to become the new leader
            foreach (var handle in waitHandles)
                handle.WaitOne(5000);

            WriteLock first = nodes[0];
            dumpNodes(count);

            // lets assert that the first election is the leader
            Assert.IsTrue(first.Owner, "The first znode should be the leader " + first.Id);

            for (int i = 1; i < count; i++)
            {
                WriteLock node = nodes[i];
                Assert.False(node.Owner, "Node should not be the leader " + node.Id);
            }

            if (count > 1)
            {
                LOG.Debug("Now killing the leader");
                // now lets kill the leader
                var firstReleased = new ManualResetEvent(false);
                var secondAcquired = new ManualResetEvent(false);
                first.LockReleased += () => firstReleased.Set();
                WriteLock second = nodes[1];
                second.LockAcquired += () => secondAcquired.Set();
                first.Unlock();
                firstReleased.WaitOne(5000);
                secondAcquired.WaitOne(5000);
                dumpNodes(count);
                // lets assert that the first election is the leader
                Assert.IsTrue(second.Owner, "The second znode should be the leader " + second.Id);

                for (int i = 2; i < count; i++)
                {
                    WriteLock node = nodes[i];
                    Assert.False(node.Owner, "Node should not be the leader " + node.Id);
                }
            }
        }

        protected void dumpNodes(int count)
        {
            for (int i = 0; i < count; i++)
            {
                WriteLock node = nodes[i];
                LOG.Debug("node: " + i + " id: " + node.Id + " is leader: " + node.Owner);
            }
        }

        [TearDown]
        protected void tearDown()
        {
            if (nodes != null)
            {
                for (int i = 0; i < nodes.Length; i++)
                {
                    WriteLock node = nodes[i];
                    if (node == null) continue;

                    LOG.Debug("Closing node: " + i);
                    node.Dispose();
                    if (i == nodes.Length - 1)
                    {
                        LOG.Debug("Not closing zookeeper: " + i + " due to bug!");
                    }
                    else
                    {
                        LOG.Debug("Closing zookeeper: " + i);
                        node.Zookeeper.Dispose();
                        LOG.Debug("Closed zookeeper: " + i);
                    }
                }
            }
        }
    }
}
