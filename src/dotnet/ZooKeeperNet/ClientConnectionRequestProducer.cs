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

        private class PacketReadState
        {
            public int? Length { get; set; }
            public byte[] Buffer { get; set; }

            public bool HaveLength
            {
                get { return Length != null; }
            }

            public int BytesWritten { get; set; }

            public PacketReadState()
            {
                Buffer = new byte[4];
            }

            public void ProcessLength(SocketAsyncEventArgs e)
            {
                using (EndianBinaryReader reader = new EndianBinaryReader(EndianBitConverter.Big,new MemoryStream(Buffer),Encoding.UTF8))
                {
                    int len = reader.ReadInt32();
                    if (len < 0 || len >= ClientConnection.packetLen)
                    {
                        throw new IOException("Packet len " + len + " is out of range!");
                    }
                    Buffer = new byte[len];
                    Length = len;
                }
                e.SetBuffer(Buffer, 0, Length.Value);
            }

            public void PacketProcessed(SocketAsyncEventArgs e)
            {
                Length = null;
                Buffer = new byte[4];
                e.SetBuffer(Buffer, 0, 4);
            }
        }

        private class SocketAsyncEventArgsPool
        {
            private readonly Stack<SocketAsyncEventArgs> _pool;

            public int Capacity
            {
                get;
                private set; 
            }

            // Initializes the object pool to the specified size
            //
            // The "capacity" parameter is the maximum number of 
            // SocketAsyncEventArgs objects the pool can hold
            public SocketAsyncEventArgsPool(int capacity)
            {
                Capacity = capacity;
                _pool = new Stack<SocketAsyncEventArgs>(capacity);
            }

            // Add a SocketAsyncEventArg instance to the pool
            //
            //The "item" parameter is the SocketAsyncEventArgs instance 
            // to add to the pool
            public void Push(SocketAsyncEventArgs item)
            {
                if (item == null)
                {
                    throw new ArgumentNullException("Items added to a SocketAsyncEventArgsPool cannot be null");
                }
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

        private readonly ManualResetEventSlim _shutdownHandle = new ManualResetEventSlim();
        private DateTime _lastHeard;
        private DateTime _lastSend;
        private readonly SocketAsyncEventArgsPool _readerPool = new SocketAsyncEventArgsPool(1);
        private readonly SocketAsyncEventArgsPool _writerPool = new SocketAsyncEventArgsPool(5);
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
            var threadStart = new SafeThreadStart(SendRequests);
            threadStart.OnUnhandledException += (t, e) =>
            {
                _shutdownHandle.Set();
                requestThread.Join();
            };
            requestThread = new Thread(threadStart.Run)
            { Name = "ZK-SendThread" + conn.zooKeeper.Id,
                IsBackground = true
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
            
            //lock here for XID?
            if (h.Type != (int)OpCode.Ping && h.Type != (int)OpCode.Auth)
            {
                h.Xid = Xid;
            }

            Packet p = new Packet(h,r,request,response,null,watchRegistration,clientPath,serverPath);
            p.clientPath = clientPath;
            p.serverPath = serverPath;

            if (!zooKeeper.State.IsAlive())
            {
                ConLossPacket(p);
            }
            else
            {
                lock (outgoingQueueLock)
                {
                    outgoingQueue.AddLast(p);
                }
            }
            EnableWrite();
            return p;        
        }

        private void InitAsyncConnections()
        {
            SocketAsyncEventArgs readWriteEventArgs;

            foreach(var i in (Enumerable.Range(0,_readerPool.Capacity)))
            {
                readWriteEventArgs = new SocketAsyncEventArgs();
                readWriteEventArgs.Completed += SendReceiveCompleted;
                readWriteEventArgs.SetBuffer(new byte[4], 0, 4);
                readWriteEventArgs.UserToken = new PacketReadState();
                _readerPool.Push(readWriteEventArgs);
            }

            

            foreach(var i in (Enumerable.Range(0, _writerPool.Capacity)))
            {
                readWriteEventArgs = new SocketAsyncEventArgs();
                readWriteEventArgs.Completed += SendReceiveCompleted;
                _writerPool.Push(readWriteEventArgs);
            }
        }

        public void SendRequests()
        {
            DateTime now = DateTime.UtcNow;
            _lastHeard = now;
            _lastSend = now;
            int counter = 0;
            InitAsyncConnections();
            while (zooKeeper.State.IsAlive())
            {
                now = DateTime.UtcNow;
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
                        _lastSend = now;
                        _lastHeard = now;
                    }
                    TimeSpan idleRecv = now - _lastHeard;
                    TimeSpan idleSend = now - _lastSend;
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
                        TimeSpan timeToNextPing = new TimeSpan(0, 0, 0, 0, Convert.ToInt32(conn.readTimeout.TotalMilliseconds / 2 - idleSend.TotalMilliseconds));
                        if (timeToNextPing <= TimeSpan.Zero)
                        {
                            SendPing();
                            _lastSend = now;
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
                    _shutdownHandle.Wait(200);
                }
                catch (Exception e)
                {
                    _shutdownHandle.Set();
                    if (conn.closing)
                    {
                        if (LOG.IsDebugEnabled)
                        {
                            // closing so this is expected
                            LOG.Debug(string.Format("An exception was thrown while closing send thread for session 0x{0:X} : {1}", conn.SessionId, e.Message));
                        }
                        //break;
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
                        conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Disconnected,EventType.None,null));
                    }
                    now = DateTime.UtcNow;
                    _lastHeard = now;
                    _lastSend = now;
                    client = null;
                }
            }   
            Cleanup();
            if (zooKeeper.State.IsAlive())
            {   
                conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Disconnected,EventType.None,null));
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
                    Thread.Sleep(new TimeSpan(0,0,0,0,random.Next(0, 50)));
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
            client.LingerState = new LingerOption(false,0);
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
            using (EndianBinaryWriter writer = new EndianBinaryWriter(EndianBitConverter.Big,ms,Encoding.UTF8))
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
                    var sw = new SetWatches(lastZxid,zooKeeper.DataWatches,zooKeeper.ExistWatches,zooKeeper.ChildWatches);
                    var h = new RequestHeader();
                    h.Type = (int)OpCode.SetWatches;
                    h.Xid = -8;
                    Packet packet = new Packet(h,new ReplyHeader(),sw,null,null,null,null,null);
                    outgoingQueue.AddFirst(packet);
                }

                foreach (ClientConnection.AuthData id in conn.authInfo)
                {
                    outgoingQueue.AddFirst(new Packet(new RequestHeader(-4,(int)OpCode.Auth),null,new AuthPacket(0,id.scheme,id.data),null,null,null,null,null));
                }
                outgoingQueue.AddFirst((new Packet(null,null,null,null,buffer,null,null,null)));
            }   

        
            EnableWrite();
        
            if (LOG.IsDebugEnabled)
            {
                LOG.Debug("Session establishment request sent on " + client.Client.RemoteEndPoint);
            }
        }

        private void SendPing()
        {
            //lastPingSentNs = DateTime.UtcNow.Nanos();
            RequestHeader h = new RequestHeader(-2,(int)OpCode.Ping);
            conn.QueuePacket(h, null, null, null, null, null, null, null, null);
            WriteAsync();
        }


        private void ReadAsync()
        {
            var e = _readerPool.Pop();
            
            if(!client.Client.ReceiveAsync(e))
                ProcessRead(e);
        }

        private void WriteAsync()
        {
        
            if (ClientIsReady() && !outgoingQueue.IsEmpty())
            {
            
                _lastSend = DateTime.UtcNow;
                Packet first = null;
                lock(outgoingQueueLock)
                {
                    if(!outgoingQueue.IsEmpty())
                        first = outgoingQueue.First.Value;
                }
                
                if(first == null)
                    return;

                var e = _writerPool.Pop();
                e.UserToken = first;
                e.SetBuffer(first.data, 0, first.data.Length);
                if (first.header != null && first.header.Type != (int)OpCode.Ping &&
                    first.header.Type != (int)OpCode.Auth)
                {
                    pendingQueue.AddLast(first);
                }
                
                if(!client.Client.SendAsync(e))
                    ProcessWrite(e);

                Interlocked.Increment(ref sentCount);
            }
        }

        private bool ClientIsReady()
        {
            return client != null && client.Client != null && client.Client.Connected;
        }

        private void SendReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            if(e.LastOperation == SocketAsyncOperation.Receive)
                ProcessRead(e);
            else if(e.LastOperation == SocketAsyncOperation.Send)
                 ProcessWrite(e);
            else
                CloseClientSocket(e);
        }

        private void ProcessWrite(SocketAsyncEventArgs e)
        {
            if (e.SocketError == SocketError.Success)
            {
                Packet first = (Packet)e.UserToken;
                System.Diagnostics.Debug.WriteLine("Wrote packet: "+first.ToString());
                if(!outgoingQueue.IsEmpty())
                {
                    lock(outgoingQueueLock)
                    {
                        if(!outgoingQueue.IsEmpty() && outgoingQueue.First() == first)  
                            outgoingQueue.RemoveFirst();
                    }
                }
                _writerPool.Push(e);
                if (outgoingQueue.IsEmpty())
                {
                    DisableWrite();
                }
                else
                {
                    EnableWrite();
                }
            }
            else
            {
                lock(outgoingQueueLock)
                {
                    if(e.UserToken != null)
                    {
                        outgoingQueue.AddFirst((Packet)e.UserToken);
                    }
                }
                _writerPool.Push(e);
            }
        }

        private void ProcessRead(SocketAsyncEventArgs e)
        {
            if(e.BytesTransferred > 0 && e.SocketError == SocketError.Success)
            {
                var state = (e.UserToken as PacketReadState) ?? new PacketReadState();
                e.UserToken = state;

                if(!state.HaveLength || (state.HaveLength && state.Length == e.BytesTransferred))
                {
                    Array.Copy(e.Buffer, 0, state.Buffer, 0, e.BytesTransferred);
                    DoRead(e,state);
                }
                else
                {
                    Array.Copy(e.Buffer,e.Offset,state.Buffer,state.BytesWritten,e.BytesTransferred);
                    state.BytesWritten = state.BytesWritten + e.BytesTransferred;
                    if(state.BytesWritten == state.Length)
                    {
                        DoRead(e,state);
                    }
                    else
                    {
                        var bytesLeft = state.Length.Value - state.BytesWritten;
                        e.SetBuffer(new byte[bytesLeft],0,bytesLeft);
                        if(!client.Client.ReceiveAsync(e))
                            ProcessRead(e);
                    }
                }
            }
            else
            {
                CloseClientSocket(e);
            }
        }

        private void DoRead(SocketAsyncEventArgs e, PacketReadState state)
        {
            _lastHeard = DateTime.UtcNow;
            if (state.Length == null)
            {
                Interlocked.Increment(ref recvCount);
                state.ProcessLength(e);
                System.Diagnostics.Debug.WriteLine("Received Length Packet: "+state.Length);
            }
            else if (!initialized)
            {                
                ReadConnectResult(state.Buffer);
                System.Diagnostics.Debug.WriteLine("Received Connection Packet");
                if (!outgoingQueue.IsEmpty()) EnableWrite();
                state.PacketProcessed(e);              
                initialized = true;                
            }
            else
            {
                ReadResponse(state.Buffer);
                state.PacketProcessed(e);                          
            }
            PrepareForNextRead(e);
        }

        private void PrepareForNextRead(SocketAsyncEventArgs e)
        {
            if(ClientIsReady() && !client.Client.ReceiveAsync(e))
                ProcessRead(e);
        }
        private void CloseClientSocket(SocketAsyncEventArgs e)
        {
            _readerPool.Push(e);
            _shutdownHandle.Set();
        }

        private void ReadConnectResult(byte[] incomingBuffer)
        {
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big,new MemoryStream(incomingBuffer),Encoding.UTF8))
            {
                BinaryInputArchive bbia = BinaryInputArchive.GetArchive(reader);
                ConnectResponse conRsp = new ConnectResponse();
                conRsp.Deserialize(bbia, "connect");
                negotiatedSessionTimeout = conRsp.TimeOut;
                if (negotiatedSessionTimeout <= 0)
                {
                    zooKeeper.State = ZooKeeper.States.CLOSED;
                    conn.consumer.QueueEvent(new WatchedEvent(KeeperState.Expired,EventType.None,null));
                    throw new SessionExpiredException(string.Format("Unable to reconnect to ZooKeeper service, session 0x{0:X} has expired", conn.SessionId));
                }
                conn.readTimeout = new TimeSpan(0,0,0,0,negotiatedSessionTimeout * 2 / 3);
                conn.connectTimeout = new TimeSpan(0,0,0,negotiatedSessionTimeout / conn.serverAddrs.Count);
                conn.SessionId = conRsp.SessionId;
                conn.SessionPassword = conRsp.Passwd;
                zooKeeper.State = ZooKeeper.States.CONNECTED;
                LOG.Info(string.Format("Session establishment complete on server {0:X}, negotiated timeout = {1}", conn.SessionId, negotiatedSessionTimeout));
                conn.consumer.QueueEvent(new WatchedEvent(KeeperState.SyncConnected,EventType.None,null));
            }
        }

        private void ReadResponse(byte[] incomingBuffer)
        {
            using (MemoryStream ms = new MemoryStream(incomingBuffer))
            using (var reader = new EndianBinaryReader(EndianBitConverter.Big,ms,Encoding.UTF8))
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
                    System.Diagnostics.Debug.WriteLine("Received packet: "+packet.GetHashCode()+" "+packet.ToString());
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
            WriteAsync();
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
            System.Diagnostics.Debug.WriteLine("Finishing Packet: " + p.GetHashCode() + " " + p.ToString());
            p.Finished = true;
            conn.consumer.QueuePacket(p);
        }

        public void Dispose()
        {
            zooKeeper.State = ZooKeeper.States.CLOSED;
            _shutdownHandle.Set();
            _shutdownHandle.Dispose();
            if(client != null)
                ((IDisposable)client).Dispose();
            requestThread.Join();
        }
    }
}
