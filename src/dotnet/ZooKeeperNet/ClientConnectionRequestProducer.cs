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
using System;
using ZooKeeperNet.IO;

namespace ZooKeeperNet
{
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Runtime.CompilerServices;
    using System.Text;
    using System.Threading;
    using log4net;
    using Org.Apache.Jute;
    using Org.Apache.Zookeeper.Proto;
    using System.Linq;

    public class ClientConnectionRequestProducer : IStartable, IDisposable
    {
        class SocketAsyncEventArgsPool
        {
            readonly Stack<SocketAsyncEventArgs> _pool;

            // Initializes the object pool to the specified size
            //
            // The "capacity" parameter is the maximum number of 
            // SocketAsyncEventArgs objects the pool can hold
            public SocketAsyncEventArgsPool(int capacity)
            {
                _pool = new Stack<SocketAsyncEventArgs>(capacity);
            }

            // Add a SocketAsyncEventArg instance to the pool
            //
            //The "item" parameter is the SocketAsyncEventArgs instance 
            // to add to the pool
            public void Push(SocketAsyncEventArgs item)
            {
                if (item == null) { throw new ArgumentNullException("Items added to a SocketAsyncEventArgsPool cannot be null"); }
                lock (_pool)
                {
                    _pool.Push(item);
                }
            }

            // Removes a SocketAsyncEventArgs instance from the pool
            // and returns the object removed from the pool
            public SocketAsyncEventArgs Pop()
            {
                lock (_pool)
                {
                    return _pool.Pop();
                }
            }

            // The number of SocketAsyncEventArgs instances in the pool
            public int Count
            {
                get { return _pool.Count; }
            }

        }

        class BufferManager
        {
            int _numBytes;                 // the total number of bytes controlled by the buffer pool
            byte[] _buffer;                // the underlying byte array maintained by the Buffer Manager
            Stack<int> _freeIndexPool;     // 
            int _currentIndex;
            int _bufferSize;

            public BufferManager(int totalBytes, int bufferSize)
            {
                _numBytes = totalBytes;
                _currentIndex = 0;
                _bufferSize = bufferSize;
                _freeIndexPool = new Stack<int>();
            }

            // Allocates buffer space used by the buffer pool
            public void InitBuffer()
            {
                // create one big large buffer and divide that 
                // out to each SocketAsyncEventArg object
                _buffer = new byte[_numBytes];
            }

            // Assigns a buffer from the buffer pool to the 
            // specified SocketAsyncEventArgs object
            //
            // <returns>true if the buffer was successfully set, else false</returns>
            public bool SetBuffer(SocketAsyncEventArgs args)
            {

                if (_freeIndexPool.Count > 0)
                {
                    args.SetBuffer(_buffer, _freeIndexPool.Pop(), _bufferSize);
                }
                else
                {
                    if ((_numBytes - _bufferSize) < _currentIndex)
                    {
                        return false;
                    }
                    args.SetBuffer(_buffer, _currentIndex, _bufferSize);
                    _currentIndex += _bufferSize;
                }
                return true;
            }

            // Removes the buffer from a SocketAsyncEventArg object.  
            // This frees the buffer back to the buffer pool
            public void FreeBuffer(SocketAsyncEventArgs args)
            {
                _freeIndexPool.Push(args.Offset);
                args.SetBuffer(null, 0, 0);
            }

        }

        private DateTime _lastHeard;
        private SocketAsyncEventArgsPool _readerPool = new SocketAsyncEventArgsPool(1);
        private static readonly ILog LOG = LogManager.GetLogger(typeof(ClientConnectionRequestProducer));
        private const string RETRY_CONN_MSG = ", closing socket connection and attempting reconnect";

        private readonly ClientConnection conn;
        private readonly ZooKeeper zooKeeper;
        private readonly Thread requestThread;
        private readonly object pendingQueueLock = new object();
        internal readonly LinkedList<Packet> pendingQueue = new LinkedList<Packet>();
        private readonly object outgoingQueueLock = new object();
        internal readonly LinkedList<Packet> outgoingQueue = new LinkedList<Packet>();
        private bool writeEnabled;

        private TcpClient client;
        private int lastConnectIndex;
        private readonly Random random = new Random();
        private int nextAddrToTry;
        private int currentConnectIndex;
        private bool initialized;
        internal long lastZxid;
        //private long lastPingSentNs;
        internal int xid = 1;

        private byte[] lenBuffer;
        private byte[] incomingBuffer = new byte[4];
        internal int sentCount;
        internal int recvCount;
        internal int negotiatedSessionTimeout;

        /// <summary>
        /// Polling timeout in microseconds
        /// </summary>
        private const int PollingTimeout = 1000;

        public ClientConnectionRequestProducer(ClientConnection conn)
        {
            this.conn = conn;
            zooKeeper = conn.zooKeeper;
            requestThread = new Thread(new SafeThreadStart(SendRequests).Run)
            { Name = "ZK-SendThread" + conn.zooKeeper.Id, IsBackground = true
            };
        }

        protected int Xid
        {
            get { return xid++; }
        }

        public void Start()
        {
            zooKeeper.State = ZooKeeper.States.CONNECTING;
            requestThread.Start();
        }

        public Packet QueuePacket(RequestHeader h, ReplyHeader r, IRecord request, IRecord response, string clientPath, string serverPath, ZooKeeper.WatchRegistration watchRegistration)
        {
            lock (outgoingQueueLock)
            {
                //lock here for XID?
                if (h.Type != (int)OpCode.Ping && h.Type != (int)OpCode.Auth)
                {
                    h.Xid = Xid;
                }

                Packet p = new Packet(h,
                r,
                request,
                response,
                null,
                watchRegistration,
                clientPath,
                serverPath);
                p.clientPath = clientPath;
                p.serverPath = serverPath;

                if (!zooKeeper.State.IsAlive())
                    ConLossPacket(p);
                else
                outgoingQueue.AddLast(p);

            return p;
            }
        }

        private void InitAsyncConnections()
        {
            SocketAsyncEventArgs readWriteEventArgs;

            readWriteEventArgs = new SocketAsyncEventArgs();
            readWriteEventArgs.Completed += SendReceiveCompleted;
            readWriteEventArgs.UserToken = Guid.NewGuid();
            readWriteEventArgs.SetBuffer(new byte[4],0,4);

            _readerPool.Push(readWriteEventArgs);
            
        }
        public void SendRequests()
        {
            DateTime now = DateTime.UtcNow;
            _lastHeard = now;
            DateTime lastSend = now;
            int counter = 0;
            InitAsyncConnections();
            while (zooKeeper.State.IsAlive())
            {
                LOG.Debug("Running main send request thread loop again " + (counter++));
                try
                {
                    if (client == null)
                    {
                        // don't re-establish connection if we are closing
                        if (conn.closing)
                        {
                            break;
                        }
                        StartConnect();
                        lastSend = now;
                        _lastHeard = now;
                    }   
                    TimeSpan idleRecv = now - _lastHeard;
                    TimeSpan idleSend = now - lastSend;
                    TimeSpan to = conn.readTimeout - idleRecv;
                    if (zooKeeper.State != ZooKeeper.States.CONNECTED)
                    {
                        to = conn.connectTimeout - idleRecv;
                    }
                    if (to <= TimeSpan.Zero)
                    {
                        throw new SessionTimeoutException(
                        string.Format("Client session timed out, have not heard from server in {0}ms for sessionid 0x{1:X}", idleRecv, conn.SessionId));
                    }
                    if (zooKeeper.State == ZooKeeper.States.CONNECTED)
                    {
                        TimeSpan timeToNextPing = new TimeSpan(0,0,0,0,Convert.ToInt32(conn.readTimeout.TotalMilliseconds / 2 - idleSend.TotalMilliseconds));
                        if (timeToNextPing <= TimeSpan.Zero)
                        {
                            SendPing();
                            lastSend = now;
                            EnableWrite();
                        }
                        else
                        {
                            if (timeToNextPing < to)
                            {
                                to = timeToNextPing;
                            }
                        }
                    }
    
                    // Everything below and until we get back to the select is
                    // non blocking, so time is effectively a constant. That is
                    // Why we just have to do this once, here
                    now = DateTime.UtcNow;
    
                    if (outgoingQueue.Count > 0)
                    {
                        // We have something to send so it's the same
                        // as if we do the send now.
                        lastSend = now;
                    }
                    if (doIO(to))
                    {
                        _lastHeard = now;
                    }
                    if (zooKeeper.State == ZooKeeper.States.CONNECTED)
                    {
                        if (outgoingQueue.Count > 0)
                        {
                            EnableWrite();
                        }
                        else
                        {
                            DisableWrite();
                        }
                    }
                }
                catch (Exception e)
                {
                    if (conn.closing)
                    {
                        if (LOG.IsDebugEnabled)
                        {
                            // closing so this is expected
                            LOG.Debug(string.Format("An exception was thrown while closing send thread for session 0x{0:X} : {1}", conn.SessionId, e.Message));
                        }
                        break;
                    }
                    // this is ugly, you have a better way speak up
                    if (e is KeeperException.SessionExpiredException)
                    {
                        LOG.Info(e.Message + ", closing socket connection");
                    }
                    else if (e is SessionTimeoutException)
                    {
                            LOG.Info(e.Message + RETRY_CONN_MSG);
                    }
                    else if (e is System.IO.EndOfStreamException)
                    {
                        LOG.Info(e.Message + RETRY_CONN_MSG);
                    }
                    else
                    {
                        LOG.Warn(string.Format("Session 0x{0:X} for server {1}, unexpected error{2}", conn.SessionId, null, RETRY_CONN_MSG), e);
                    }
                    Cleanup();
                    if (zooKeeper.State.IsAlive())
                    {
                        conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Disconnected,
                        EventType.None,
                        null));
                    }
    
                    now = DateTime.UtcNow;
                    _lastHeard = now;
                    lastSend = now;
                    client = null;
                }
            }
            Cleanup();
            if (zooKeeper.State.IsAlive())
            {
                conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Disconnected,
                EventType.None,
                null));
            }
            if (LOG.IsDebugEnabled) LOG.Debug("SendThread exitedloop.");
        }
    
        private void Cleanup()
        {
            if (client != null)
            {
                try
                {
                    client.Client.Close();
                    client.Close();
                }
                catch (IOException e)
                {
                    if (LOG.IsDebugEnabled)
                    {
                        LOG.Debug("Ignoring exception during channel close", e);
                    }
                }
            }
            try
            {
                Thread.Sleep(100);
            }
            catch (ThreadInterruptedException e)
            {
                if (LOG.IsDebugEnabled)
                {
                    LOG.Debug("SendThread interrupted during sleep, ignoring");
                }
            }
    
            lock (pendingQueueLock)
            {
                foreach (Packet p in pendingQueue)
                {
                    ConLossPacket(p);
                }
                pendingQueue.Clear();
            }
    
            lock (outgoingQueueLock)
            {
                foreach (Packet p in outgoingQueue)
                {
                    ConLossPacket(p);
                }
                outgoingQueue.Clear();
            }
        }
    
        private void StartConnect()
        {
            if (lastConnectIndex == -1)
            {
                // We don't want to delay the first try at a connect, so we
                // start with -1 the first time around
                lastConnectIndex = 0;
            }
            else
            {
                try
                {
                    Thread.Sleep(new TimeSpan(0,
                    0,
                    0,
                    0,
                    random.Next(0, 50)));
                }
                catch (ThreadInterruptedException e1)
                {
                    LOG.Warn("Unexpected exception", e1);
                }
                if (nextAddrToTry == lastConnectIndex)
                {
                    try
                    {
                        // Try not to spin too fast!
                        Thread.Sleep(1000);
                    }
                    catch (ThreadInterruptedException e)
                    {
                        LOG.Warn("Unexpected exception", e);
                    }
                }
            }
            zooKeeper.State = ZooKeeper.States.CONNECTING;
            currentConnectIndex = nextAddrToTry;
            IPEndPoint addr = conn.serverAddrs[nextAddrToTry];
            nextAddrToTry++;
            if (nextAddrToTry == conn.serverAddrs.Count)
            {
                nextAddrToTry = 0;
            }
            LOG.Info("Opening socket connection to server " + addr);
            client = new TcpClient();
            client.LingerState = new LingerOption(false,
            0);
            client.NoDelay = true;
            ConnectSocket(addr);
    
            //sock.Blocking = true;
            PrimeConnection(client);
            initialized = false;
            ReadAsync();
        }
    
        private void ConnectSocket(IPEndPoint addr)
        {
            bool connected = false;
            ManualResetEvent socketConnectTimeout = new ManualResetEvent(false);
            ThreadPool.QueueUserWorkItem(state =>
            {
                try
                {
                    client.Connect(addr);
                    connected = true;
                    socketConnectTimeout.Set();
                }
                // ReSharper disable EmptyGeneralCatchClause
                catch
                // ReSharper restore EmptyGeneralCatchClause
                {
                }
            });
            socketConnectTimeout.WaitOne(10000);
            if (connected) return;
    
            throw new InvalidOperationException(string.Format("Could not make socket connection to {0}:{1}", addr.Address, addr.Port));
        }
    
        private void PrimeConnection(TcpClient client)
        {
            LOG.Info(string.Format("Socket connection established to {0}, initiating session", client.Client.RemoteEndPoint));
            lastConnectIndex = currentConnectIndex;
            ConnectRequest conReq = new ConnectRequest(0,
            lastZxid,
            Convert.ToInt32(conn.SessionTimeout.TotalMilliseconds),
            conn.SessionId,
            conn.SessionPassword);

            byte[] buffer;
            using (MemoryStream ms = new MemoryStream())
            using (EndianBinaryWriter writer = new EndianBinaryWriter(EndianBitConverter.Big,
            ms,
            Encoding.UTF8))
            {
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(writer);
                boa.WriteInt(-1, "len");
                conReq.Serialize(boa, "connect");
                ms.Position = 0;
                writer.Write(ms.ToArray().Length - 4);
                buffer = ms.ToArray();
            }
            lock (outgoingQueueLock)
            {
                if (!ClientConnection.disableAutoWatchReset && (!zooKeeper.DataWatches.IsEmpty() || !zooKeeper.ExistWatches.IsEmpty() || !zooKeeper.ChildWatches.IsEmpty()))
                {
                    var sw = new SetWatches(lastZxid,
                    zooKeeper.DataWatches,
                    zooKeeper.ExistWatches,
                    zooKeeper.ChildWatches);
                    var h = new RequestHeader();
                    h.Type = (int)OpCode.SetWatches;
                    h.Xid = -8;
                    Packet packet = new Packet(h,
                    new ReplyHeader(),
                    sw,
                    null,
                    null,
                    null,
                    null,
                    null);
                    outgoingQueue.AddFirst(packet);
                }

                foreach (ClientConnection.AuthData id in conn.authInfo)
                {
                    outgoingQueue.AddFirst(new Packet(new RequestHeader(-4,
                    (int)OpCode.Auth),
                    null,
                    new AuthPacket(0,
                    id.scheme,
                    id.data),
                    null,
                    null,
                    null,
                    null,
                    null));
                }
                outgoingQueue.AddFirst((new Packet(null,
                null,
                null,
                null,
                buffer,
                null,
                null,
                null)));
            }

            lock (this)
            {
                EnableWrite();
            }

            if (LOG.IsDebugEnabled)
            {
                LOG.Debug("Session establishment request sent on " + client.Client.RemoteEndPoint);
            }
        }

        private void SendPing()
        {
            //lastPingSentNs = DateTime.UtcNow.Nanos();
            RequestHeader h = new RequestHeader(-2,
            (int)OpCode.Ping);
            conn.QueuePacket(h, null, null, null, null, null, null, null, null);
        }

        private bool doIO(TimeSpan to)
        {
            LOG.Debug("doIO (" + to.TotalMilliseconds + "ms)");
            bool packetReceived = false;
            if (client == null) throw new IOException("Socket is null!");

            //packetReceived = TryRead();

            TryWrite();
            return packetReceived;
        }

        private void ReadAsync()
        {
            //return;
            
            var e = _readerPool.Pop();
            
            if(!client.Client.ReceiveAsync(e))
                ProcessRead(e);
        }

        private void SendReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            if(e.LastOperation == SocketAsyncOperation.Receive)
                ProcessRead(e);
            else
                CloseClientSocket(e);
        }

        private void ProcessRead(SocketAsyncEventArgs e)
        {
            if(e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                var buffer = new byte[e.BytesTransferred];
                Array.Copy(e.Buffer,e.Offset,buffer,0,e.BytesTransferred);

                if(lenBuffer == null)
                {
                    recvCount++;
                    var newBuffer = GetLength(buffer);
                    e.SetBuffer(newBuffer, 0, newBuffer.Length);                    
                }
                else if(!initialized)
                {
                    ReadConnectResult(buffer);
                    if (!outgoingQueue.IsEmpty()) EnableWrite();
                    lenBuffer = null;
                    e.SetBuffer(new byte[4],0,4);
                    initialized = true;
                }
                else
                {
                    ReadResponse(buffer);
                    lenBuffer = null;
                    e.SetBuffer(new byte[4],0,4);
                }
                _lastHeard = DateTime.UtcNow;
                
                if(!client.Client.ReceiveAsync(e))
                    ProcessRead(e);
                TryWrite();
            }
            else
            {
                CloseClientSocket(e);
            }
        }
        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            _readerPool.Push(e);
        }

        private bool TryRead()
        {
            if (client.Client.Poll(PollingTimeout, SelectMode.SelectRead))
            {
                int total = 0;
                int current = total = client.GetStream().Read(incomingBuffer, total, incomingBuffer.Length - total);

                while (total < incomingBuffer.Length && current > 0)
                {
                    current = client.GetStream().Read(incomingBuffer, total, incomingBuffer.Length - total);
                    total += current;
                }

                if (current <= 0)
                {
                    throw new EndOfStreamException(string.Format("Unable to read additional data from server sessionid 0x{0:X}, likely server has closed socket",
                    conn.SessionId));
                }

                if (lenBuffer == null)
                {
                    lenBuffer = incomingBuffer;
                    recvCount++;
                    ReadLength();
                }
                else
                if (!initialized)
                {
                    ReadConnectResult(incomingBuffer);
                    if (!outgoingQueue.IsEmpty()) EnableWrite();
                    lenBuffer = null;
                    incomingBuffer = new byte[4];
                    initialized = true;
                }
                else
                {
                    ReadResponse(incomingBuffer);
                    lenBuffer = null;
                    incomingBuffer = new byte[4];
                }
                return true;
            }
            return false;
        }
        private void TryWrite()
        {
            if (writeEnabled && client.Client.Poll(PollingTimeout, SelectMode.SelectWrite))
            {
                lock (outgoingQueueLock)
                {
                    if (!outgoingQueue.IsEmpty())
                    {
                        Packet first = outgoingQueue.First.Value;
                        client.GetStream().Write(first.data, 0, first.data.Length);
                        sentCount++;
                        outgoingQueue.RemoveFirst();
                        if (first.header != null && first.header.Type != (int)OpCode.Ping &&
                        first.header.Type != (int)OpCode.Auth)
                        {
                            pendingQueue.AddLast(first);
                        }
                    }
                }
            }

            if (outgoingQueue.IsEmpty())
            {
                DisableWrite();
            }
            else
            {
                EnableWrite();
            }
        }
        private byte[] GetLength(byte[] buffer)
        {
            lenBuffer = new byte[4];
            using (EndianBinaryReader reader = new EndianBinaryReader(EndianBitConverter.Big,
            new MemoryStream(buffer),
            Encoding.UTF8))
            {
                int len = reader.ReadInt32();
                if (len < 0 || len >= ClientConnection.packetLen)
                {
                    throw new IOException("Packet len " + len + " is out of range!");
                }
                return new byte[len];
            }
        }
        private void ReadLength()
        {
            lenBuffer = new byte[4];
            using (EndianBinaryReader reader = new EndianBinaryReader(EndianBitConverter.Big,
            new MemoryStream(incomingBuffer),
            Encoding.UTF8))
            {
                int len = reader.ReadInt32();
                if (len < 0 || len >= ClientConnection.packetLen)
                {
                    throw new IOException("Packet len " + len + " is out of range!");
                }
                incomingBuffer = new byte[len];
            }
        }

        private void ReadConnectResult(byte[] incomingBuffer)
        {
            string[] byteString = incomingBuffer.Select(b => b.ToString("X2")).ToArray();
            System.Diagnostics.Debug.WriteLine(string.Format("Reading Connect Result: {0}", string.Join(" ", byteString)));
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big,
            new MemoryStream(incomingBuffer),
            Encoding.UTF8))
            {
                BinaryInputArchive bbia = BinaryInputArchive.GetArchive(reader);
                ConnectResponse conRsp = new ConnectResponse();
                conRsp.Deserialize(bbia, "connect");
                negotiatedSessionTimeout = conRsp.TimeOut;
                if (negotiatedSessionTimeout <= 0)
                {
                    zooKeeper.State = ZooKeeper.States.CLOSED;
                    conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Expired,
                    EventType.None,
                    null));
                    throw new SessionExpiredException(string.Format("Unable to reconnect to ZooKeeper service, session 0x{0:X} has expired", conn.SessionId));
                }
                conn.readTimeout = new TimeSpan(0,
                0,
                0,
                0,
                negotiatedSessionTimeout * 2 / 3);
                conn.connectTimeout = new TimeSpan(0,
                0,
                0,
                negotiatedSessionTimeout / conn.serverAddrs.Count);
                conn.SessionId = conRsp.SessionId;
                conn.SessionPassword = conRsp.Passwd;
                zooKeeper.State = ZooKeeper.States.CONNECTED;
                LOG.Info(string.Format("Session establishment complete on server {0:X}, negotiated timeout = {1}", conn.SessionId, negotiatedSessionTimeout));
                conn.consumer.QueueEvent(new WatchedEvent(KeeperState.SyncConnected,
                EventType.None,
                null));
            }
        }

        private void ReadResponse(byte[] incomingBuffer)
        {
            string[] byteString = incomingBuffer.Select(b => b.ToString("X2")).ToArray();
            System.Diagnostics.Debug.WriteLine(string.Format("Reading Response: {0}", string.Join(" ", byteString)));
            using (MemoryStream ms = new MemoryStream(incomingBuffer))
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big,
            ms,
            Encoding.UTF8))
            {
                BinaryInputArchive bbia = BinaryInputArchive.GetArchive(reader);
                ReplyHeader replyHdr = new ReplyHeader();

                replyHdr.Deserialize(bbia, "header");
                if (replyHdr.Xid == -2)
                {
                    // -2 is the xid for pings
                    return;
                }
                if (replyHdr.Xid == -4)
                {
                    // -2 is the xid for AuthPacket
                    // TODO: process AuthPacket here
                    if (LOG.IsDebugEnabled)
                    {
                        LOG.Debug(string.Format("Got auth sessionid:0x{0:X}", conn.SessionId));
                    }
                    return;
                }
                if (replyHdr.Xid == -1)
                {
                    // -1 means notification
                    if (LOG.IsDebugEnabled)
                    {
                        LOG.Debug(string.Format("Got notification sessionid:0x{0}", conn.SessionId));
                    }
                    WatcherEvent @event = new WatcherEvent();
                    @event.Deserialize(bbia, "response");

                    // convert from a server path to a client path
                    if (conn.ChrootPath != null)
                    {
                        string serverPath = @event.Path;
                        if (serverPath.CompareTo(conn.ChrootPath) == 0)
                            @event.Path = "/";
                        else
                        @event.Path = serverPath.Substring(conn.ChrootPath.Length);
                    }

                    WatchedEvent we = new WatchedEvent(@event);
                    if (LOG.IsDebugEnabled)
                    {
                        LOG.Debug(string.Format("Got {0} for sessionid 0x{1:X}", we, conn.SessionId));
                    }

                    conn.consumer.QueueEvent(we);
                    return;
                }
                if (pendingQueue.IsEmpty())
                {
                    throw new IOException(string.Format("Nothing in the queue, but got {0}", replyHdr.Xid));
                }
                Packet packet;
                lock (pendingQueueLock)
                {
                    packet = pendingQueue.First.Value;
                    pendingQueue.RemoveFirst();
                }
                /*
                     * Since requests are processed in order, we better get a response
                     * to the first request!
                     */
                try
                {
                    if (packet.header.Xid != replyHdr.Xid)
                    {
                        packet.replyHeader.Err = (int)KeeperException.Code.CONNECTIONLOSS;
                        throw new IOException(string.Format("Xid out of order. Got {0} expected {1}", replyHdr.Xid, packet.header.Xid));
                    }

                    packet.replyHeader.Xid = replyHdr.Xid;
                    packet.replyHeader.Err = replyHdr.Err;
                    packet.replyHeader.Zxid = replyHdr.Zxid;
                    if (replyHdr.Zxid > 0)
                    {
                        lastZxid = replyHdr.Zxid;
                    }
                    if (packet.response != null && replyHdr.Err == 0)
                    {
                        packet.response.Deserialize(bbia, "response");
                    }

                    if (LOG.IsDebugEnabled)
                    {
                        LOG.Debug(string.Format("Reading reply sessionid:0x{0:X}, packet:: {1}", conn.SessionId, packet));
                    }
                }
                finally
                {
                    FinishPacket(packet);
                }
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void EnableWrite()
        {
            writeEnabled = true;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void DisableWrite()
        {
            writeEnabled = false;
        }

        private void ConLossPacket(Packet p)
        {
            if (p.replyHeader == null) return;
            string state = zooKeeper.State.State;
            if (state == ZooKeeper.States.AUTH_FAILED.State)
                p.replyHeader.Err = (int)KeeperException.Code.AUTHFAILED;
            else
                if (state == ZooKeeper.States.CLOSED.State)
                    p.replyHeader.Err = (int)KeeperException.Code.SESSIONEXPIRED;
                else
                p.replyHeader.Err = (int)KeeperException.Code.CONNECTIONLOSS;

            FinishPacket(p);
        }

        private void FinishPacket(Packet p)
        {
            if (p.watchRegistration != null)
            {
                p.watchRegistration.Register(p.replyHeader.Err);
            }

            p.Finished = true;
            conn.consumer.QueuePacket(p);
        }

        public void Dispose()
        {
            zooKeeper.State = ZooKeeper.States.CLOSED;
            requestThread.Join();
        }
    }
}
