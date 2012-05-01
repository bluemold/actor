package bluemold.actor

import collection.immutable.HashMap
import java.net._
import annotation.tailrec
import bluemold.concurrent.{CancelableQueue, AtomicBoolean}
import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ConcurrentHashMap}
import java.nio.charset.Charset

case class LocalNodeIdentity( appName: String, groupName: String )

object UDPNode {
  private[actor] val broadcastPort = 9900
  private[actor] val minPort = 9901
  private[actor] val maxPort = 9999

  private[actor] val sendingChunkSize = 1024.shortValue()
  private[actor] val maxMissingList = 256

  val nodes = new ConcurrentHashMap[LocalNodeIdentity,UDPNode]

  def getNode( appName: String, groupName: String ) = {
    val localId = LocalNodeIdentity( appName, groupName )
    val node = nodes.get( localId )
    if ( node == null ) {
      val node = new UDPNode( localId )
      val oldNode = nodes.putIfAbsent( localId, node )
      if ( oldNode == null ) { node.startup(); node }
      else oldNode
    } else node
  }
}

case class UDPAddress( address: InetAddress, port: Int )

final class UDPNode( localId: LocalNodeIdentity ) extends Node { selfNode =>
  import UDPNode._

  val UTF8 = Charset.forName("utf8")
  val done = new AtomicBoolean()

  val pollTimeout = 100
  val waitingForReceiptTimeout = 200
  val maxReceiptWaits = 5
  val waitingAfterReceiptTimeout = 10000
  val sent = new ConcurrentHashMap[UUID,SendingMessage]
  val sentToProcess = new LinkedBlockingQueue[SendingMessage]
  val sentInWaiting = new CancelableQueue[SendingMessage]
  val sentCompleted = new LinkedBlockingQueue[SendingMessage]
  
  val waitingForAllChunksTimeout = 100
  val maxChunkWaits = 5
  val waitingAfterCompleteTimeout = 60000
  val received = new ConcurrentHashMap[UUID,ReceivingMessage]
  val receivedInWaiting = new CancelableQueue[ReceivingMessage]
  val receivedCompleted = new LinkedBlockingQueue[ReceivingMessage]
  val receiveBufferSize = 2 * 1024 * 1024
  val sendBufferSize = 2 * 1024 * 1024
  
  val rateLimitDuration = 25L
  val rateLimitWait = 1L
  val maxRateLimit = 600

  var showNetworkUntil = 0L

  def showNetworkSnapshot( duration: Long ) {
    showNetworkUntil = System.currentTimeMillis() + duration 
  }

  private def startSendingMessage( message: SendingMessage ) {
    println( "TRACE: SendingMessage: " + message )
    sent.put( message.uuid, message )
    sentToProcess.add( message )
  }

  private def startReceivingMessage( message: ReceivingMessage ): ReceivingMessage = {
    println( "TRACE: ReceivingMessage: " + message )
    val oldMessage = received.putIfAbsent( message.uuid, message )
    if ( oldMessage == null ) {
      message.startWaiting()
      message
    } else oldMessage
  }

  private def getSendingMessage( uuid: UUID ) = sent.get( uuid )  
  private def getReceivingMessage( uuid: UUID ) = received.get( uuid )


  def getInterfaces = getSockets.foldLeft(Nil: List[NodeInterface]) { ( list, op ) =>
    op match {
      case (iAddress,(interface,socket,bSocket,address)) => interface :: list
    }
  }

  @volatile var sockets: HashMap[InterfaceAddress,(UDPNodeInterface,DatagramSocket,DatagramSocket,InetAddress)] = _

  private def getSockets = {
    if ( sockets == null ) {
      synchronized {
        if ( sockets == null ) {
          sockets = getSockets0( NetworkInterface.getNetworkInterfaces )
        }
      }
    }
    sockets
  }

  private def getSockets0( interfaces: java.util.Enumeration[NetworkInterface] ): HashMap[InterfaceAddress,(UDPNodeInterface,DatagramSocket,DatagramSocket,InetAddress)] = {
    var map = new HashMap[InterfaceAddress,(UDPNodeInterface,DatagramSocket,DatagramSocket,InetAddress)]()
    while ( interfaces.hasMoreElements ) {
      val interface = interfaces.nextElement()
      val addresses = interface.getInterfaceAddresses.iterator()
      while ( addresses.hasNext ) {
        val address = addresses.next()
        val socket = getSocket( minPort, address )
        if ( socket != null ) {
          val iAddress = address.getAddress
          val broadcastAddress = address.getBroadcast
          if ( broadcastAddress != null ) {
            val broadcastSocket = new DatagramSocket( null: SocketAddress )
            broadcastSocket.setReuseAddress( true )
            broadcastSocket.setReceiveBufferSize( receiveBufferSize )
            broadcastSocket.bind( new InetSocketAddress( iAddress, broadcastPort ) )

            val nodeInterface = UDPNodeInterface( interface, iAddress )
            val broadcastReSender = Actor.actorOf(new ReSender()).start().asInstanceOf[LocalActorRef]
            val broadcastReceiver = new Thread( new Receiver( broadcastSocket, nodeInterface, broadcastReSender ), "UDPNode-" + getAppName + "-Broadcast-" + iAddress + ":" + broadcastPort )
            broadcastReceiver.setDaemon( true )
            broadcastReceiver.start()

            val socketReSender = Actor.actorOf(new ReSender()).start().asInstanceOf[LocalActorRef]
            val socketReceiver = new Thread( new Receiver( socket, nodeInterface, socketReSender ), "UDPNode-" + getAppName + "-Receiver-" + iAddress + ":" + socket.getLocalPort )
            socketReceiver.setDaemon( true )
            socketReceiver.start()

            println( "addr: " + address.getAddress + "bcast: " + broadcastAddress )
            map += ((address,((nodeInterface,socket,broadcastSocket,broadcastAddress))))
          }
        }
      }
    }
    map
  }

  private def getSocket( port: Int, address: InterfaceAddress ): DatagramSocket = {
    if( port >= minPort && port <= maxPort ) {
      try {
        val socket = new DatagramSocket( port, address.getAddress )
        socket.setBroadcast( true )
        socket.setReceiveBufferSize( receiveBufferSize )
        socket.setSendBufferSize( sendBufferSize )
        socket
      } catch { case _ => getSocket( port + 1, address ) }
    } else null
  }

  def getAppName: String = localId.appName
  def getGroupName: String = localId.groupName

  val addressToId: ConcurrentHashMap[UDPAddress,NodeIdentity] = new ConcurrentHashMap[UDPAddress,NodeIdentity]
  val idToAddresses: ConcurrentHashMap[NodeIdentity,List[UDPAddress]] = new ConcurrentHashMap[NodeIdentity, List[UDPAddress]]

  def getDestination( nodeId: NodeIdentity ): Option[(UDPAddress,DatagramSocket)] = {
    val target: UDPAddress = if ( nodeId != null ) {
      val addys = idToAddresses.get( nodeId )
      if ( addys != null ) {
        val (locals,others) = addys partition { _.address.isLoopbackAddress  }
        if ( locals.isEmpty ) {
          // todo choose fastest interface else load balance
          if ( others.isEmpty ) null else others.head
        } else locals.head
      } else null
    } else null
    
    if ( target != null ) {
      val socket = getSocketForTarget( target )
      if ( socket != null ) Some((target,socket)) else None
    } else None
  } 

  def getSocketForTarget( target: UDPAddress ): DatagramSocket = {
    var socket: DatagramSocket = null
    val sockets = getSockets
    for ( address <- sockets.keys ) {
      if ( socket == null ) {
        val targetBytes = target.address.getAddress
        val interfaceBytes = address.getAddress.getAddress
        var prefixLen: Int = address.getNetworkPrefixLength
        if ( targetBytes.length == interfaceBytes.length ) {
          var same = true
          for ( i <- 0 until targetBytes.length ) {
            if ( prefixLen >= 8 ) {
              if ( targetBytes(i) != interfaceBytes(i) )
                same = false
              prefixLen -= 8
            } else if ( prefixLen > 0 ) {
              if ( ( targetBytes(i) >>> ( 8 - prefixLen ) ) != ( interfaceBytes(i) >>> ( 8 - prefixLen ) ) )
                same == false
              prefixLen = 0
            }
          }
          if ( same ) {
            sockets.get(address) match {
              case Some( ( interface, aSocket, aBroadcast, bAddress ) ) => socket = aSocket
              case None =>
            }
          }
        }
      }
    }
    socket
  } 

  def startup() {
    getSockets
    val sender = new Thread( new Sender(), "UDPNode-" + getAppName + "-Sender" )
    sender.setDaemon( true )
    sender.start()
    val sentWaiting = new Thread( new SentWaitingProcessor(), "UDPNode-" + getAppName + "-Sent-Waiting" )
    sentWaiting.setDaemon( true )
    sentWaiting.start()
    val sentCleaner = new Thread( new SentCompletedCleaner(), "UDPNode-" + getAppName + "-Sent-Completed" )
    sentCleaner.setDaemon( true )
    sentCleaner.start()
    val receivedWaiting = new Thread( new ReceivedWaitingProcessor(), "UDPNode-" + getAppName + "-Received-Waiting" )
    receivedWaiting.setDaemon( true )
    receivedWaiting.start()
    val receivedCleaner = new Thread( new ReceivedCompletedCleaner(), "UDPNode-" + getAppName + "-Received-Completed" )
    receivedCleaner.setDaemon( true )
    receivedCleaner.start()
  }

  def shutdown() {
    done.set( true )
    nodes.remove( localId, this )
    for ( (interface,socket,broadcast,bAddress) <- getSockets.values ) {
      try {
        socket.close()
      } catch { case t: Throwable => t.printStackTrace() }
      try {
        broadcast.close()
      } catch { case t: Throwable => t.printStackTrace() }
    }
  }

  private def send( nodeId: NodeIdentity, out: ByteArrayOutputStream, sender: LocalActorRef ) {
    val bytes = out.toByteArray
    val msg = new SendingMessage( bytes, nodeId, sendingChunkSize, sender )
    startSendingMessage( msg )
  } 

  private def serialize( msg: Any ): Array[Byte] = {
    val out = new ByteArrayOutputStream
    Node.forSerialization.set( UDPNode.this )
    try {
      val objectOut = new ObjectOutputStream( out )
      objectOut.writeObject( msg )
      objectOut.flush()
    } finally {
      Node.forSerialization.remove()
    }
    out.toByteArray
  }

  private def send( nodeId: NodeIdentity, message:NodeMessage, sender: LocalActorRef ) {
    val out = new ByteArrayOutputStream
    NodeMessage.serialize( out, message )
    send( nodeId, out, sender )
  }

  private def send(uuid: UUID, msg: Any, sender: UUID, localActorRef: LocalActorRef ) {
    val bytes = serialize( msg )
    val route = new NodeRoute( uuid.nodeId, sender.nodeId :: Nil )
    send( uuid.nodeId, ActorNodeMessage( route, sender, uuid, bytes, 0, bytes.length ), localActorRef )
  }

  def send(uuid: UUID, msg: Any, sender: UUID ) {
    val bytes = serialize( msg )
    val route = new NodeRoute( uuid.nodeId, sender.nodeId :: Nil )
    send( uuid.nodeId, ActorNodeMessage( route, sender, uuid, bytes, 0, bytes.length ), null )
  }

  def send(uuid: UUID, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( uuid, msg, localActorRef._getUUID, localActorRef )
  }

  def sendAll(nodeId: NodeIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null ) {
      val bytes = serialize( msg )
      val route = new NodeRoute( nodeId, sender._getUUID.nodeId :: Nil )
      send( nodeId, ActorNodeAllMessage( route, sender._getUUID, className, bytes, 0, bytes.length ), localActorRef )
    }
  }

  def sendAllWithId(nodeId: NodeIdentity, id: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null ) {
      val bytes = serialize( msg )
      val route = new NodeRoute( nodeId, sender._getUUID.nodeId :: Nil )
      send( nodeId, ActorNodeMessageById( route, sender._getUUID, id, bytes, 0, bytes.length  ), localActorRef )
    }
  }

  // todo - route parameter needs to be used
  def sendAll(route: List[NodeIdentity], nodeId: NodeIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null ) {
      val bytes = serialize( msg )
      val route = new NodeRoute( nodeId, sender._getUUID.nodeId :: Nil )
      send( nodeId, ActorNodeAllMessage( route, sender._getUUID, className, bytes, 0, bytes.length ), localActorRef )
    }
  }

  // todo - route parameter needs to be used
  def sendAllWithId(route: List[NodeIdentity], nodeId: NodeIdentity, id: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null ) {
      val bytes = serialize( msg )
      val route = new NodeRoute( nodeId, sender._getUUID.nodeId :: Nil )
      send( nodeId, ActorNodeMessageById( route, sender._getUUID, id, bytes, 0, bytes.length ), localActorRef )
    }
  }

  def stopRemoteActor(target: UUID, sender: LocalActorRef) {
    // todo
  }

  def requestRemoteStatus(target: UUID, sender: LocalActorRef) {
    // todo
  }

  def updateNodeAddressMap( nodeId: NodeIdentity, address: InetAddress, port: Int ) {
    updateNodeAddressMap0( nodeId, UDPAddress( address, port ) )
  }

  @tailrec
  def updateNodeAddressMap0( nodeId: NodeIdentity, udpAddress: UDPAddress ) {
    idToAddresses.get( nodeId ) match {
      case addys: List[UDPAddress] =>
        if ( ! addys.contains( udpAddress ) && ! idToAddresses.replace( nodeId, addys, udpAddress :: addys ) )
          updateNodeAddressMap0( nodeId, udpAddress )
      case null =>
        if ( idToAddresses.putIfAbsent( nodeId, udpAddress :: Nil ) != null )
          updateNodeAddressMap0( nodeId, udpAddress )
    }
    
  }

  final class Sender extends Runnable {
    def run() {
      while ( ! done.get() ) {
        try {
          val message = sentToProcess.poll( pollTimeout, TimeUnit.MILLISECONDS )
          if ( message != null ) {
            if ( message.status == NotSent ) {
              message.send()
              message.markSentAndWait()
            }
          }
        } catch { case t: Throwable => t.printStackTrace() } // log and try again
      }
    }
  }
  
  def getTypeNum( mType: Byte ) = mType % 16

  final class Receiver( socket: DatagramSocket, nodeInterface: UDPNodeInterface, reSender: LocalActorRef ) extends Runnable {
    def run() {
      val buffer = new Array[Byte](16384)
      val packet = new DatagramPacket(buffer,buffer.length)
      while ( ! done.get() ) {
        try {
          socket.receive( packet )
          val data = packet.getData
          val dataOffset = packet.getOffset
          val dataLength = packet.getLength
          val in = new ByteArrayInputStream( data, dataOffset, dataLength )
          val mType = readByte( in )
          val uuid = new UUID( new NodeIdentity( readLong( in ), readLong( in ) ), readLong( in ), readLong( in ) )
          val destination = { val time = readLong( in ); val rand = readLong( in ); if ( time != 0 || rand != 0 ) new NodeIdentity( time, rand ) else null }
          val totalSize = readInt( in )
          val chunkSize = readShort( in )
          
          getTypeNum( mType ) match {
            case 1 | 2 | 6 =>
              updateNodeAddressMap( uuid.nodeId, packet.getAddress, packet.getPort )

              if ( destination == null || destination == getNodeId ) {
                val receivingMessage = {
                  val receivingMessage = getReceivingMessage( uuid )
                  if ( receivingMessage != null ) receivingMessage
                  else startReceivingMessage( new ReceivingMessage( uuid, totalSize, chunkSize, destination ) )
                }
  
                getTypeNum( mType ) match {
                  case 1 => // MessageChunk => type(B)(1) + uuid(obj) + destCID + total size(int32) + chunk size(int16) + index( start with zero )(int32) + data(B[])
                    val index = readInt( in )
                    if ( showNetworkUntil > System.currentTimeMillis() )
                      println( "Chunk:   " + uuid + " : " + index + " : " + packet.getAddress + " : " + packet.getPort + " to " + socket.getLocalAddress + " : " + socket.getLocalPort )
                    val remainder = totalSize - index * chunkSize
                    val dataSize = if ( remainder < chunkSize ) remainder else chunkSize
                    receivingMessage.addChunk( index, data, dataOffset + 59, dataSize ) // offset = 1 + 32 + 16 + 4 + 2 + 4
                    if ( receivingMessage.isComplete ) {
                      receivingMessage.processMessageOnce( packet.getAddress, packet.getPort, nodeInterface, reSender )
                    }
                  case 2 => // MessageReceiptRequest => type(B)(2) + uuid + total size(int32) + chunk size(int16)
                    if ( destination == getNodeId ) {
                      if ( receivingMessage.isComplete )
                        receivingMessage.sendReceiptResponse( packet.getAddress, packet.getPort )
                      else
                        receivingMessage.requestMissingChunks( packet.getAddress, packet.getPort )
                    } // todo: else there is an error somewhere. Broadcast messages don't ask for receipts.
                  case 6 => // MessageNoLongerExists => type(B)(6) + uuid + total size(int32) + chunk size(int16)
                    // todo: report error
                  case _ => throw new RuntimeException( "What Happened!" )
                }
              }
            case 3 | 4 | 5 =>
              val sendingMessage = getSendingMessage( uuid )

              getTypeNum( mType ) match {
                case 3 => // MessageReceipt => type(B)(3) + uuid + total size(int32) + chunk size(int16) + errorCode ( 0=success, 1=failure )(int16)
                  if ( sendingMessage == null ) {
//                    println( "ReceiptF:" + uuid + " : " + packet.getAddress + " : " + packet.getPort + " to " + socket.getLocalAddress + " : " + socket.getLocalPort )
                    // todo report error
                  } else {
//                    println( "Receipt: " + uuid + " : " + packet.getAddress + " : " + packet.getPort + " to " + socket.getLocalAddress + " : " + socket.getLocalPort )
                    sendingMessage.markReceived()
                  }
                case 4 => // MessageChunksNeeded => type(B)(4) + uuid + total size(int32) + chunk size(int16) + num ids (int16) + List( id, id, id, id, id )
                  if ( sendingMessage == null ) {
                    sendMessageNotFound( uuid, totalSize, chunkSize, packet.getAddress, packet.getPort )
                  } else {
                    val num = readShort( in )
                    var missing: List[Int] = Nil
                    0 until num foreach { i => missing ::= readInt( in ) }
                    sendingMessage.resend( missing )
                  }
                case 5 => // MessageChunkRangesNeeded => type(B)(5) + uuid + total size(int32) + chunk size(int16) + num ids (int16) + List( id - id, id - id, id - id )
                  if ( sendingMessage == null ) {
                    sendMessageNotFound( uuid, totalSize, chunkSize, packet.getAddress, packet.getPort )
                  } else {
                    val num = readShort( in )
                    var missing: List[Int] = Nil
                    0 until num foreach { i => missing :::= ( readInt( in ) to readInt( in ) ).toList }
                    sendingMessage.resend( missing )
                  }
                case _ => throw new RuntimeException( "What Happened!" )
              }
            case _ => throw new RuntimeException( "What Happened! Bad IO? " )
          }
          
        } catch { case t: Throwable => t.printStackTrace() } // log and try again
      }
    }
  }

  private def deSerialize( bytes: Array[Byte], off: Int, len: Int, actor: BaseRegisteredActor ): Any = {
    val classLoader = actor.getCurrentStrategy().getClassLoader
    Node.forSerialization.set( UDPNode.this )
    try {
      new ObjectInputStream( new ByteArrayInputStream( bytes, off, len ) ) {
        override def resolveClass( desc: ObjectStreamClass ) = Class.forName( desc.getName, false, classLoader )
      }.readObject()
    } finally {
      Node.forSerialization.remove()
    }
  }
  private def sendMessageToLocalActor( base: BaseRegisteredActor, sender: UUID, nodeInterface: UDPNodeInterface, msg: Array[Byte], off: Int, len: Int ) {
    base match {
      case actor: BaseRegisteredActor =>
        val decodedMsg = deSerialize( msg, off, len, actor )
        actor._actor match {
          case target: InterfaceRestrictedActor =>
            if ( target.isInterfaceAllowed( nodeInterface ) )
              actor._localRef.!(decodedMsg)(new RemoteActorRef(sender,UDPNode.this))
          case _ => actor._localRef.!(decodedMsg)(new RemoteActorRef(sender,UDPNode.this))
        }
      case _ => // ignore
    }
  }
  private def processMessage( nodeMessage: NodeMessage, nodeInterface: UDPNodeInterface, reSender: LocalActorRef ) {
    if ( nodeMessage.destination == null || nodeMessage.destination == _nodeId ) {
      nodeMessage match {
          // Todo: Stop and Status should also respect InterfaceRestrictedActor.
        case StopActorNodeMessage(  route, sender, recipient ) =>
          val actor = getByUUID( recipient )
          if ( actor != null )
            actor.stop()
        case StatusRequestNodeMessage( route, sender, recipient ) =>
          getByUUID( recipient ) match {
            case actor: LocalActorRef =>
              // todo
            case _ => // ignore
          }
        case StatusResponseNodeMessage( route, sender, recipient, stopped ) =>
          // todo
          // sendMessageToLocalActor( getBaseByUUID( recipient ), stopped, sender, nodeInterface )
        case ActorNodeMessage( route, sender, recipient, msg, off, len ) =>
          if ( recipient == null )
            for ( actor <- getAllBase )
              sendMessageToLocalActor( actor, sender, nodeInterface, msg, off, len )
          else sendMessageToLocalActor( getBaseByUUID( recipient ), sender, nodeInterface, msg, off, len )
        case ActorNodeAllMessage( route, sender, className, msg, off, len ) =>
          for ( actor <- if ( className == null ) getAllBase else getAllBaseByClassName( className ) )
            sendMessageToLocalActor( actor, sender, nodeInterface, msg, off, len )
        case ActorNodeMessageById( route, sender, id, msg, off, len ) =>
          for ( actor <- if ( id == null ) getAllBase else getAllBaseById( id ) )
            sendMessageToLocalActor( actor, sender, nodeInterface, msg, off, len )
        case _ => throw new RuntimeException( "What Happened!" )
      }
    } else {
      nodeMessage.destination.path match {
        case nextHop :: tail => send( nextHop, nodeMessage, reSender )
        case _ => throw new RuntimeException( "What Happened!" )
      }
    }
  }
  private class ReSender extends RegisteredActor {
    protected def init() {}
    protected def react = {
      case _ => // todo: resender logic
    }
  }
  def writeByte( out: OutputStream, value: Byte ) { out.write( value ) }
  def writeShort( out: OutputStream, value: Short ) {
    var temp: Int = value
    out.write( temp & 0xFF )
    temp >>>= 8; out.write( temp & 0xFF )
  }
  def writeInt( out: OutputStream, value: Int ) {
    var temp = value
    out.write( temp & 0xFF )
    temp >>>= 8; out.write( temp & 0xFF )
    temp >>>= 8; out.write( temp & 0xFF )
    temp >>>= 8; out.write( temp & 0xFF )
  }
  def writeLong( out: OutputStream, value: Long ) {
    var temp = value
    out.write( ( temp & 0xFF ).asInstanceOf[Int] ); 
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
    temp >>>= 8; out.write( ( temp & 0xFF ).asInstanceOf[Int] )
  }
  def writeString( out: OutputStream, value: String ) {
    val bytes = value.getBytes( UTF8 )
    writeInt( out, bytes.length )
    out.write( bytes )
  }
  def readByte( in: InputStream ): Byte = in.read().asInstanceOf[Byte]
  def readShort( in: InputStream ): Short = {
    var temp: Short = in.read().asInstanceOf[Short]
    temp = ( temp + in.read() * 0x100 ).asInstanceOf[Short]
    temp
  }
  def readInt( in: InputStream ): Int = {
    var temp: Int = in.read()
    temp += in.read() * 0x100
    temp += in.read() * 0x10000
    temp += in.read() * 0x1000000
    temp
  }
  def readLong( in: InputStream ): Long = {
    var temp: Long = in.read().asInstanceOf[Long]
    temp += ( in.read().asInstanceOf[Long] ) * 0x100L
    temp += ( in.read().asInstanceOf[Long] ) * 0x10000L
    temp += ( in.read().asInstanceOf[Long] ) * 0x1000000L
    temp += ( in.read().asInstanceOf[Long] ) * 0x100000000L
    temp += ( in.read().asInstanceOf[Long] ) * 0x10000000000L
    temp += ( in.read().asInstanceOf[Long] ) * 0x1000000000000L
    temp += ( in.read().asInstanceOf[Long] ) * 0x100000000000000L
    temp
  }
  def readString( in: InputStream ): ( String, Int ) = {
    val len = readInt( in )
    val bytes = new Array[Byte]( len )
    in.read( bytes )
    ( new String( bytes, UTF8 ), len )
  }
  def sendMessageNotFound( uuid: UUID, totalSize: Int, chunkSize: Short, address: InetAddress, port: Int ) {
    if ( showNetworkUntil > System.currentTimeMillis() )
      println( "MessageNotFound: " + uuid + " : " + address + " : " + port )
    selfNode.synchronized {
      rateLimit()
    val socket = getSocketForTarget( new UDPAddress( address, port ) )
    if ( socket != null ) {
      val out = new ByteArrayOutputStream()
      writeByte(out,6) // MessageNoLongerExists => type(B)(6) + uuid + total size(int32) + chunk size(int16)
      writeLong(out,uuid.nodeId.time)
      writeLong(out,uuid.nodeId.rand)
      writeLong(out,uuid.time)
      writeLong(out,uuid.rand)
      writeInt(out,totalSize)
      writeShort(out,chunkSize)
      out.flush()
      val packetBytes = out.toByteArray
      val packet = new DatagramPacket(packetBytes,packetBytes.length,address,port)
      try {
        socket.send(packet)
        // TODO: report errors
      } catch { case t: Throwable => t.printStackTrace() }
    } // todo: else what?
    }
  }

  sealed abstract class SendingStatus
  case object NotSent extends SendingStatus
  case object WaitingForReceipt extends SendingStatus
  case object SuccessfullySent extends SendingStatus

  var rateLimitStartTime = 0L
  var rateLimitCount = 0
  
  def rateLimit() {
    synchronized {
      val now = System.currentTimeMillis()
      if ( now > rateLimitStartTime + rateLimitDuration ) {
        rateLimitStartTime = now
        rateLimitCount = 0
      }
      while ( rateLimitCount > maxRateLimit ) {
        wait( rateLimitWait )
        val now = System.currentTimeMillis()
        if ( now > rateLimitStartTime + rateLimitDuration ) {
          rateLimitStartTime = now
          rateLimitCount = 0
        }
      }
      rateLimitCount += 1
    }
  }
  
  final class SendingMessage( _bytes: Array[Byte], destination: NodeIdentity, chunkSize: Short, sender: LocalActorRef ) {
    val uuid = new UUID( getNodeId )
    val bytes = _bytes
    val totalSize = bytes.length
    
    def isBroadCast = destination == null

    var status: SendingStatus = _
    var waitingEntry: CancelableQueue.Entry[SendingMessage] = _ 
    var waitTill: Long = _
    var waitRepeatedCount: Int = _

    synchronized {
      status = NotSent
    }

    def totalChunks = ( if ( totalSize % chunkSize > 0 ) totalSize / chunkSize + 1 else totalSize / chunkSize )

    def getWaitTill: Long = synchronized { waitTill }

    def startWaiting() {
      synchronized {
        if ( waitingEntry == null || ! waitingEntry.isInList )
          waitingEntry = sentInWaiting.push( this )
        waitTill = System.currentTimeMillis() + waitingForReceiptTimeout
      }
    }

    def resetWaiting() {
      synchronized {
        if ( waitingEntry != null && waitingEntry.isInList )
          sentInWaiting.delete( waitingEntry )
        waitingEntry = sentInWaiting.push( this )
        waitTill = System.currentTimeMillis() + waitingForReceiptTimeout
      }
    }

    def stopWaiting() {
      synchronized {
        if ( waitingEntry != null && waitingEntry.isInList )
          sentInWaiting.delete( waitingEntry )
      }
    }

    def markReceived() {
      synchronized {
        status = SuccessfullySent
        stopWaiting()
        waitTill = System.currentTimeMillis() + waitingAfterReceiptTimeout
        sentCompleted.add( this )
      }
    }

    def markSentAndWait() {
      synchronized {
        if ( status == NotSent ) {
          if ( destination == null ) {
            status = SuccessfullySent
            waitTill = System.currentTimeMillis() + waitingAfterReceiptTimeout
            sentCompleted.add( this )
          } else {
            status = WaitingForReceipt
            startWaiting()
          }
        }
      }
    }

    def send() {
      getDestination( destination ) match {
        case Some((udpAddress,socket)) =>
          send( socket, udpAddress.address,udpAddress.port )
        case None =>
          for ( (interface,socket,broadcast,bAddress) <- getSockets.values )
            send( socket,bAddress,broadcastPort )
      }
    }
    
    def resend( chunks: List[Int] ) {
      getDestination( destination ) match {
        case Some((udpAddress,socket)) =>
          chunks foreach { send( socket, udpAddress.address,udpAddress.port, _ ) }
        case None =>
          for ( (interface,socket,broadcast,bAddress) <- getSockets.values )
            chunks foreach { send( socket,bAddress,broadcastPort, _ ) }
      }
    }

    private def send( socket: DatagramSocket, address: InetAddress, port: Int ) {
      0 until totalChunks foreach { send( socket, address, port, _ ) }
    }

    private def send( socket: DatagramSocket, address: InetAddress, port: Int, index: Int ) {
      println( "TRACE: SendingChunk: " + index + " : " + uuid + " : " + address + " : " + port )
      if ( showNetworkUntil > System.currentTimeMillis() )
        println( "SendingChunk: " + index + " : " + uuid + " : " + address + " : " + port )
      selfNode.synchronized {
        rateLimit()
      val offset = index * chunkSize
      val remainder = totalSize - offset 
      val size = if ( remainder < chunkSize ) remainder else chunkSize
      val out = new ByteArrayOutputStream()
      writeByte(out,1) // MessageChunk
      writeLong(out,uuid.nodeId.time)
      writeLong(out,uuid.nodeId.rand)
      writeLong(out,uuid.time)
      writeLong(out,uuid.rand)
      if ( destination == null ) {
        writeLong(out,0L)
        writeLong(out,0L)
      } else {
        writeLong(out,destination.time)
        writeLong(out,destination.rand)
      }
      writeInt(out,totalSize)
      writeShort(out,chunkSize)
      writeInt(out,index)
      out.write(bytes,offset,size)
      out.flush()
      val packetBytes = out.toByteArray
      val packet = new DatagramPacket(packetBytes,packetBytes.length,address,port)
      try {
        socket.send(packet)
        // TODO: report errors
      } catch { case t: Throwable => t.printStackTrace() }
      }
    }

    def requestReceipt() {
      var doRequest = false
      synchronized {
        if ( status == WaitingForReceipt ) {
          if ( waitRepeatedCount < maxReceiptWaits ) {
            waitRepeatedCount += 1
            resetWaiting()
            doRequest = true
          } else {
            println( "Too many wait for receipts")
            // report and delete
            sent.remove( uuid )
            // todo: report
          }
        }
      }
      if ( doRequest ) {
        getDestination( destination ) match {
          case Some((udpAddress,socket)) =>
            requestReceipt( socket, udpAddress.address,udpAddress.port )
          case None =>
            for ( (interface,socket,broadcast,bAddress) <- getSockets.values )
              requestReceipt( socket,bAddress,broadcastPort )
        }
      }
    }

    private def requestReceipt( socket: DatagramSocket, address: InetAddress, port: Int ) {
      if ( showNetworkUntil > System.currentTimeMillis() )
        println( "RequestReceipt: " + uuid + " : " + address + " : " + port )
      selfNode.synchronized {
        rateLimit()
      
      val out = new ByteArrayOutputStream()
      writeByte(out,2) // MessageReceiptRequest => type(B)(2) + uuid + destCID + total size(int32) + chunk size(int16)
      writeLong(out,uuid.nodeId.time)
      writeLong(out,uuid.nodeId.rand)
      writeLong(out,uuid.time)
      writeLong(out,uuid.rand)
      if ( destination == null ) {
        writeLong(out,0L)
        writeLong(out,0L)
      } else {
        writeLong(out,destination.time)
        writeLong(out,destination.rand)
      }
      writeInt(out,totalSize)
      writeShort(out,chunkSize)
      out.flush()
      val packetBytes = out.toByteArray
      val packet = new DatagramPacket(packetBytes,packetBytes.length,address,port)
      try {
        socket.send(packet)
        // TODO: report errors
      } catch { case t: Throwable => t.printStackTrace() }
      }
    }
  }

  sealed abstract class ReceivingStatus
  case object WaitingForChunks extends ReceivingStatus
  case object SuccessfullyReceived extends ReceivingStatus

  final class ReceivingMessage( _uuid: UUID, totalSize: Int, chunkSize: Short, destination: NodeIdentity ) {
    val bytes = new Array[Byte]( totalSize )
    def uuid = _uuid

    var chunks: List[Int] = Nil
    var message: NodeMessage = _
    var messageProcessed: Boolean = _

    var status: ReceivingStatus = _
    var waitingEntry: CancelableQueue.Entry[ReceivingMessage] = _ 
    var waitTill: Long = _
    var waitRepeatedCount: Int = _

    synchronized {
      status = WaitingForChunks
    }

    def getWaitTill: Long = synchronized { waitTill }

    def startWaiting() {
      synchronized {
        if ( waitingEntry == null || ! waitingEntry.isInList )
          waitingEntry = receivedInWaiting.push( this )
        waitTill = System.currentTimeMillis() + waitingForAllChunksTimeout
      }
    }

    def resetWaiting() {
      synchronized {
        if ( waitingEntry != null && waitingEntry.isInList )
          waitingEntry.delete()
        waitingEntry = receivedInWaiting.push( this )
        waitTill = System.currentTimeMillis() + waitingForAllChunksTimeout
      }
    }

    def stopWaiting() {
      synchronized {
        if ( waitingEntry != null && waitingEntry.isInList )
          waitingEntry.delete()
        waitTill = 0
      }
    }

    def markReceived() {
      synchronized {
//        println( "Marked Received!")
        status = SuccessfullyReceived
        stopWaiting()
        waitTill = System.currentTimeMillis() + waitingAfterReceiptTimeout
        receivedCompleted.add( this )
      }
    }
    def addChunk( index: Int, buf: Array[Byte], offset: Int, length: Int ) {
      synchronized {
        if ( ! chunks.contains( index ) ) {
         Array.copy( buf, offset, bytes, index * chunkSize, length )
         chunks ::= index
        }
        if ( isComplete )
          markReceived()
      }
    }
    
    def totalChunks = ( if ( totalSize % chunkSize > 0 ) totalSize / chunkSize + 1 else totalSize / chunkSize )
    
    def missingChunks(): List[Int] = {
      synchronized {
        ( 0 until totalChunks filter { ! chunks.contains( _ ) } ).toList
      }
    }
    
    def isComplete: Boolean = {
      synchronized {
        chunks.size == totalChunks
      }
    }
    
    def processMessageOnce( address: InetAddress, port: Int, nodeInterface: UDPNodeInterface, reSender: LocalActorRef ) = {
      synchronized {
        if ( message == null )
          message = NodeMessage.deSerialize( bytes, 0, bytes.length )
        if ( message != null && ! messageProcessed ) {
          messageProcessed = true
          if ( destination == null )
            processMessage( message, nodeInterface: UDPNodeInterface, reSender )
          else if ( destination == getNodeId ) {
            processMessage( message, nodeInterface: UDPNodeInterface, reSender )
            sendReceiptResponse( address, port )
          }
        }
        message
      }      
    }
    
    def sendReceiptResponse() {
      getDestination( uuid.nodeId ) match {
        case Some((address,socket)) =>
          sendReceiptResponse( socket, address.address, address.port )
        case None => // todo: what do we do here?
      }
    }
    def sendReceiptResponse( address: InetAddress, port: Int ) {
      val socket = getSocketForTarget( new UDPAddress( address, port ) )
      if ( socket != null )
        sendReceiptResponse( socket, address, port )
      // todo: else what?
    }
    def sendReceiptResponse( socket: DatagramSocket, address: InetAddress, port: Int ) {
      if ( showNetworkUntil > System.currentTimeMillis() )
        println( "ReceiptResponse: " + uuid + " : " + address + " : " + port )
      selfNode.synchronized {
        rateLimit()
      
      val out = new ByteArrayOutputStream()
      writeByte(out,3) // MessageReceipt => type(B)(3) + uuid + total size(int32) + chunk size(int16) + errorCode ( 0=success, 1=failure )(int16)
      writeLong(out,uuid.nodeId.time)
      writeLong(out,uuid.nodeId.rand)
      writeLong(out,uuid.time)
      writeLong(out,uuid.rand)
      if ( destination == null ) {
        writeLong(out,0L)
        writeLong(out,0L)
      } else {
        writeLong(out,destination.time)
        writeLong(out,destination.rand)
      }
      writeInt(out,totalSize)
      writeShort(out,chunkSize)
      writeShort(out,0) // success
      out.flush()
      val packetBytes = out.toByteArray
      val packet = new DatagramPacket(packetBytes,packetBytes.length,address,port)
      try {
        socket.send(packet)
        // TODO: report errors
      } catch { case t: Throwable => t.printStackTrace() }
      }
    }
    def requestMissingChunksAndWait() {
      var doRequest = false
      synchronized {
        if ( status == WaitingForChunks ) {
          if ( waitRepeatedCount < maxChunkWaits ) {
            waitRepeatedCount += 1
            resetWaiting()
            doRequest = true
          } else {
            // report and delete
            received.remove( uuid )
            // todo: report
          }
        }
      }
      if ( doRequest )
        requestMissingChunks()
    }
    def requestMissingChunks() {
      getDestination( uuid.nodeId ) match {
        case Some((address,socket)) =>
          requestMissingChunks( socket, address.address, address.port )
        case None => // todo: what do we do here?
      }
    }
    def requestMissingChunks( address: InetAddress, port: Int ) {
      val socket = getSocketForTarget( new UDPAddress( address, port ) )
      if ( socket != null )
        requestMissingChunks( socket, address, port )
      // todo: else what?
    }
    def requestMissingChunks( socket: DatagramSocket, address: InetAddress, port: Int ) {
      val missing = missingChunks()
      if ( missing.size > maxMissingList ) {
        missing.sliding(maxMissingList,maxMissingList) foreach { requestMissingChunksList( socket, address, port, _ ) }
      } else requestMissingChunksList( socket, address, port, missing )
    }
    private def requestMissingChunksList( socket: DatagramSocket, address: InetAddress, port: Int, missing: List[Int] ) {
      println( "Requesting Missing Chunks! num: " + missing.size + " : " + address )
      selfNode.synchronized {
        rateLimit()
      
      val out = new ByteArrayOutputStream()
      writeByte(out,4) // MessageChunksNeeded => type(B)(4) + uuid + total size(int32) + chunk size(int16) + num ids (int16) + List( id, id, id, id, id )
      writeLong(out,uuid.nodeId.time)
      writeLong(out,uuid.nodeId.rand)
      writeLong(out,uuid.time)
      writeLong(out,uuid.rand)
      if ( destination == null ) {
        writeLong(out,0L)
        writeLong(out,0L)
      } else {
        writeLong(out,destination.time)
        writeLong(out,destination.rand)
      }
      writeInt(out,totalSize)
      writeShort(out,chunkSize)
      writeShort(out,missing.size.asInstanceOf[Short]) // num of missing
      missing foreach { writeInt( out, _ ) }
      out.flush()
      val packetBytes = out.toByteArray
      val packet = new DatagramPacket(packetBytes,packetBytes.length,address,port)
      try {
        socket.send(packet)
        // TODO: report errors
      } catch { case t: Throwable => t.printStackTrace() }
      }
    }
  }
  /**
   * Message sending algorithm:
   * send message ( connection error may be immediate or not ) and mark successfully send chunks
   * wait for response, if no response in time report error on interface to node listener
   * then try another interface. If no more interfaces or out of tries then report message failure to node listener.
   * If actor instance of MessageFailureActor then report message as failed.
   * 
   * MessageChunk => type(B)(1) + uuid(obj) + destNID + total size(int32) + chunk size(int16) + index( start with zero )(int32) + data(B[])
   * MessageReceiptRequest => type(B)(2) + uuid + destNID + total size(int32) + chunk size(int16)
   * MessageReceipt => type(B)(3) + uuid + destNID + total size(int32) + chunk size(int16) + errorCode ( 0=success, 1=failure )(int16)
   * MessageChunksNeeded => type(B)(4) + uuid + destNID + total size(int32) + chunk size(int16) + num ids (int16) + List( id, id, id, id, id ) 
   * MessageChunkRangesNeeded => type(B)(5) + uuid + destNID + total size(int32) + chunk size(int16) + num ids (int16) + List( id - id, id - id, id - id )
   * MessageNoLongerExists => type(B)(6) + uuid + destNID + total size(int32) + chunk size(int16)
   * 
   * Receiver triggers MessageReceipt upon receiving all chunks
   * If sender does not hear back after a set time he sends a recipt request
   * Receiver replys to a recipt request with a recipt or chunks needed response.
   * The chunks needed response is sent even when the reciever never heard any of the chunks.
   * 
   * If the receiver has not gotten all the chunks by a set time they respond with a chunks needed.
   * If the receiver does not receive all the chunks by a set time then it is simply forgotten.
   */
  final class SentWaitingProcessor() extends Runnable {
    def run() {
      while ( ! done.get() ) {
        sentInWaiting.pop() match {
          case None => synchronized { wait( waitingForReceiptTimeout ) }
          case Some( message ) =>
            val currentTime = System.currentTimeMillis()
            val waitTill = message.getWaitTill
            if ( currentTime < waitTill )
              synchronized { wait( waitTill - currentTime ) }
            message.requestReceipt()
        }
      }
    }
  }

  final class SentCompletedCleaner() extends Runnable {
    def run() {
      while ( ! done.get() ) {
        try {
          val message = sentCompleted.poll( pollTimeout, TimeUnit.MILLISECONDS )
          if ( message != null ) {
            if ( message.status == SuccessfullySent ) {
              val currentTime = System.currentTimeMillis()
              val waitTill = message.getWaitTill
              if ( currentTime < waitTill )
                synchronized { wait( waitTill - currentTime ) }
              sent.remove( message.uuid )
            } else throw new RuntimeException( "What Happened!" )
          }
        } catch { case t: Throwable => t.printStackTrace() } // log and try again
      }
    }
  }

  final class ReceivedWaitingProcessor() extends Runnable {
    def run() {
      while ( ! done.get() ) {
        receivedInWaiting.pop() match {
          case None => synchronized { wait( waitingForAllChunksTimeout ) }
          case Some( message ) =>
            val currentTime = System.currentTimeMillis()
            val waitTill = message.getWaitTill
            if ( currentTime < waitTill )
              synchronized { wait( waitTill - currentTime ) }
            message.requestMissingChunksAndWait()
        }
      }
    }
  }

  final class ReceivedCompletedCleaner() extends Runnable {
    def run() {
      while ( ! done.get() ) {
        try {
          val message = receivedCompleted.poll( pollTimeout, TimeUnit.MILLISECONDS )
          if ( message != null ) {
            if ( message.status == SuccessfullyReceived ) {
              val currentTime = System.currentTimeMillis()
              val waitTill = message.getWaitTill
              if ( currentTime < waitTill )
                synchronized { wait( waitTill - currentTime ) }
              received.remove( message.uuid )
            } else throw new RuntimeException( "What Happened!" )
          }
        } catch { case t: Throwable => t.printStackTrace() } // log and try again
      }
    }
  }

  object NodeMessage {
    def deSerialize( bytes: Array[Byte], off: Int, len: Int ): NodeMessage = {
      var bytesRead = 0
      val in = new ByteArrayInputStream( bytes, off, len )
      val mType = readByte( in )
      bytesRead += 1
      val nPathLength = readInt( in )
      bytesRead += 4
      var hops: List[NodeIdentity] = Nil
      1 until nPathLength foreach { i =>
        hops ::= new NodeIdentity( readLong( in ), readLong( in ) )
        bytesRead += 16
      }
      val route = new NodeRoute( hops.head, hops.tail.reverse )
      val senderNode = route.intermediate.head
      val sender = new UUID( new NodeIdentity( senderNode.time, senderNode.rand, route.intermediate.tail ), readLong( in ), readLong( in ) )
      bytesRead += 16
      mType match {
        case 1 =>
          val recipent = new UUID( route.target, readLong( in ), readLong( in ) )
          bytesRead += 16
          StopActorNodeMessage( route, sender, recipent )
        case 2 =>
          val recipent = new UUID( route.target, readLong( in ), readLong( in ) )
          bytesRead += 16
          StatusRequestNodeMessage( route, sender, recipent )
        case 3 =>
          val recipent = new UUID( route.target, readLong( in ), readLong( in ) )
          val stopped = ! ( readByte( in ) == 0 )  
          bytesRead += 17
          StatusResponseNodeMessage( route, sender, recipent, stopped )
        case 4 =>
          val recipent = new UUID( route.target, readLong( in ), readLong( in ) )
          bytesRead += 16
          ActorNodeMessage( route, sender, recipent, bytes, off + bytesRead, len - bytesRead  )
        case 5 =>
          val ( recipentClass, len ) = readString( in )
          bytesRead += len
          ActorNodeAllMessage( route, sender, recipentClass, bytes, off + bytesRead, len - bytesRead )
        case 6 =>
          val ( recipentId, len ) = readString( in )
          bytesRead += len
          ActorNodeMessageById( route, sender, recipentId, bytes, off + bytesRead, len - bytesRead )
        case _ => throw new RuntimeException( "Invalid" )
      }
    }
    def writeBeginning( out: OutputStream, mType: Byte, route: NodeRoute, sender: UUID ) {
      writeByte( out, mType )
      val nPathLength = route.intermediate.size + 1
      writeInt( out, nPathLength )
      route.intermediate foreach { nodeId => writeLong( out, nodeId.time ); writeLong ( out, nodeId.rand ) }
      writeLong( out, route.target.time )
      writeLong( out, route.target.rand )
      writeLong( out, sender.time )
      writeLong( out, sender.rand )
    }
    def serialize( out: OutputStream, message: NodeMessage ) {
      message match {
        case StopActorNodeMessage( route, sender, recipent ) =>
          writeBeginning( out, 1, route, sender )
          writeLong( out, recipent.time )
          writeLong( out, recipent.rand )
        case StatusRequestNodeMessage( route, sender, recipent ) =>
          writeBeginning( out, 2, route, sender )
          writeLong( out, recipent.time )
          writeLong( out, recipent.rand )
        case StatusResponseNodeMessage( route, sender, recipent, stopped ) =>
          writeBeginning( out, 3, route, sender )
          writeLong( out, recipent.time )
          writeLong( out, recipent.rand )
          writeByte( out, if ( stopped ) 1 else 0 )
        case ActorNodeMessage( route, sender, recipent, bytes, off, len ) =>
          writeBeginning( out, 4, route, sender )
          writeLong( out, recipent.time )
          writeLong( out, recipent.rand )
          out.write( bytes, off, len )
        case ActorNodeAllMessage( route, sender, recipentClass, bytes, off, len ) =>
          writeBeginning( out, 5, route, sender )
          writeString( out, recipentClass )
          out.write( bytes, off, len )
        case ActorNodeMessageById( route, sender, recipentId, bytes, off, len ) =>
          writeBeginning( out, 6, route, sender )
          writeString( out, recipentId )
          out.write( bytes, off, len )
      }
    }
  }
  
  sealed abstract class NodeMessage extends Serializable {
    def route: NodeRoute
    def destination: NodeIdentity = route.target
    def source: NodeIdentity = route.intermediate.head
  }
  
  case class StopActorNodeMessage( _route: NodeRoute, sender: UUID, recipient: UUID ) extends NodeMessage { def route = _route }
  case class StatusRequestNodeMessage( _route: NodeRoute, sender: UUID, recipient: UUID ) extends NodeMessage { def route = _route }
  case class StatusResponseNodeMessage( _route: NodeRoute, sender: UUID, recipient: UUID, stopped: Boolean ) extends NodeMessage { def route = _route }
  case class ActorNodeMessage( _route: NodeRoute, sender: UUID, recipient: UUID, msg: Array[Byte], off: Int, len: Int ) extends NodeMessage { def route = _route }
  case class ActorNodeAllMessage( _route: NodeRoute, sender: UUID, className: String, msg: Array[Byte], off: Int, len: Int ) extends NodeMessage { def route = _route }
  case class ActorNodeMessageById( _route: NodeRoute, sender: UUID, id: String, msg: Array[Byte], off: Int, len: Int ) extends NodeMessage { def route = _route }
}

case class UDPNodeInterface( interface: NetworkInterface, address: InetAddress ) extends NodeInterface