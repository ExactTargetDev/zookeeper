using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using Org.Apache.Zookeeper.Data;

namespace ZooKeeperNet.Tests
{
    using NUnit.Framework;
    
    [TestFixture]
    public class IntegrationTests : AbstractZooKeeperTests
    {
        public static void Main()
        {
            new IntegrationTests().Create1000EmphemeralNodes();
        }

        [Test]
        public void Create1000EmphemeralNodes()
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            using (ZooKeeper zk = CreateClient())
            {
                Thread.Sleep(10000);
                for (var i = 0; i < 1000; i++)
                {
                    byte[] data = Encoding.ASCII.GetBytes("a" + i);
                    zk.Create("/somepath" + i, data, Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
                }

                for (var i = 0; i < 1000; i++)
                {
                    Stat stat = new Stat();
                    byte[] data = zk.GetData("/somepath" + i, new MyWatcher(), stat);
                    var result = Encoding.ASCII.GetString(data);
                    Assert.AreEqual("a" + i, result);
                    stat = zk.SetData("/somepath" + i, Encoding.ASCII.GetBytes("boo"), stat.Version);
                    zk.Delete("/somepath" + i, stat.Version);
                }
            }
            stopwatch.Stop();
            Console.WriteLine("Create1000EmphemeralNodes() took: " + stopwatch.ElapsedMilliseconds + "ms. ");
        }
    }

    public class MyWatcher : IWatcher
    {
        public void Process(WatchedEvent @event)
        {
            // do nothing
        }
    }
}
