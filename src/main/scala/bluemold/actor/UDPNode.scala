package bluemold.actor

import collection.immutable.HashMap
import java.net._
import annotation.tailrec
import bluemold.concurrent.{CancelableQueue, AtomicBoolean}
import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ConcurrentHashMap}

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

final class UDPNode( localId: LocalNodeIdentity ) extends Node {
  import UDPNode._

  val done = new AtomicBoolean()

  val pollTimeout = 200
  val waitingForReceiptTimeout = 1000
  val maxReceiptWaits = 3
  val waitingAfterReceiptTimeout = 1000
  val sent = new ConcurrentHashMap[UUID,SendingMessage]
  val sentToProcess = new LinkedBlockingQueue[SendingMessage]
  val sentInWaiting = new CancelableQueue[SendingMessage]
  val sentCompleted = new LinkedBlockingQueue[SendingMessage]
  
  val waitingForAllChunksTimeout = 1000
  val maxChunkWaits = 3
  val waitingAfterCompleteTimeout = 6000
  val received = new ConcurrentHashMap[UUID,ReceivingMessage]
  val receivedInWaiting = new CancelableQueue[ReceivingMessage]
  val receivedCompleted = new LinkedBlockingQueue[ReceivingMessage]
  
  private def startSendingMessage( message: SendingMessage ) { 
    sent.put( message.uuid, message )
    sentToProcess.add( message )
  }

  private def startReceivingMessage( message: ReceivingMessage ): ReceivingMessage = {
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
            broadcastSocket.bind( new InetSocketAddress( iAddress, broadcastPort ) )

            val nodeInterface = UDPNodeInterface( interface, iAddress )

            val broadcastReceiver = new Thread( new Receiver( broadcastSocket, nodeInterface ), "UDPNode-" + getAppName + "-Broadcast-" + iAddress + ":" + broadcastPort )
            broadcastReceiver.setDaemon( true )
            broadcastReceiver.start()

            val socketReceiver = new Thread( new Receiver( socket, nodeInterface ), "UDPNode-" + getAppName + "-Receiver-" + iAddress + ":" + socket.getLocalPort )
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
      addys match {
        case head :: tail => head
        case Nil => null
        case null => null
      }
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

  
  
  def send( nodeId: NodeIdentity, message:NodeMessage, sender: LocalActorRef ) {
    val out = new ByteArrayOutputStream
    val objectOut = new ObjectOutputStream( out )
    objectOut.writeObject( message )
    objectOut.flush()
    send( nodeId, out, sender )
  }

  private def send(uuid: UUID, msg: Any, sender: UUID, localActorRef: LocalActorRef ) {
    send( uuid.nodeId, ActorNodeMessage( uuid, msg, sender ), localActorRef )
  }

  def send(uuid: UUID, msg: Any, sender: UUID ) {
    send( uuid.nodeId, ActorNodeMessage( uuid, msg, sender ), null )
  }

  def send(uuid: UUID, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( uuid, msg, localActorRef._getUUID, localActorRef )
  }

  def sendAll(nodeId: NodeIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( nodeId, ActorNodeAllMessage( nodeId, className, msg, localActorRef._getUUID ), localActorRef )
  }

  def sendAllWithId(nodeId: NodeIdentity, id: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( nodeId, ActorNodeMessageById( nodeId, id, msg, localActorRef._getUUID ), localActorRef )
  }
  
  def sendAll(route: List[NodeIdentity], nodeId: NodeIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( nodeId, ActorNodeAllMessage( nodeId, className, msg, localActorRef._getUUID ), localActorRef )
  }

  def sendAllWithId(route: List[NodeIdentity], nodeId: NodeIdentity, id: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( nodeId, ActorNodeMessageById( nodeId, id, msg, localActorRef._getUUID ), localActorRef )
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

  final class Receiver( socket: DatagramSocket, nodeInterface: UDPNodeInterface ) extends Runnable {
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
//                    println( "Chunk:   " + uuid + " : " + index + " : " + packet.getAddress + " : " + packet.getPort + " to " + socket.getLocalAddress + " : " + socket.getLocalPort )
                    val remainder = totalSize - index * chunkSize
                    val dataSize = if ( remainder < chunkSize ) remainder else chunkSize
                    receivingMessage.addChunk( index, data, dataOffset + 59, dataSize ) // offset = 1 + 32 + 16 + 4 + 2 + 4
                    if ( receivingMessage.isComplete ) {
                      receivingMessage.processMessageOnce( packet.getAddress, packet.getPort, nodeInterface )
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

  private def sendMessageToLocalActor( base: BaseRegisteredActor, msg: Any, sender: UUID, nodeInterface: UDPNodeInterface ) {
    base match {
      case actor: BaseRegisteredActor => actor._actor match {
        case target: InterfaceRestrictedActor =>
          if ( target.isInterfaceAllowed( nodeInterface ) )
            actor._localRef.!(msg)(new RemoteActorRef(sender,UDPNode.this))
        case _ => actor._localRef.!(msg)(new RemoteActorRef(sender,UDPNode.this))
      }
      case _ => // ignore
    }
  }
  private def processMessage( nodeMessage: NodeMessage, nodeInterface: UDPNodeInterface ) {
    nodeMessage match {
        // Todo: Stop and Status should also respect InterfaceRestrictedActor.
      case StopActorNodeMessage( recipient: UUID, sender: UUID ) =>
        val actor = getByUUID( recipient )
        if ( actor != null )
          actor.stop()
      case StatusRequestNodeMessage( recipient: UUID, sender: UUID ) =>
        getByUUID( recipient ) match {
          case actor: LocalActorRef =>
            send( sender.nodeId, StatusResponseNodeMessage( sender, recipient, ! actor.isActive ), actor )
          case _ => // ignore
        }
      case StatusResponseNodeMessage( recipient: UUID, sender: UUID, stopped: Boolean ) =>
        sendMessageToLocalActor( getBaseByUUID( recipient ), stopped, sender, nodeInterface )
      case ActorNodeMessage( uuid, msg, sender ) =>
        if ( uuid == null )
          for ( actor <- getAllBase )
            sendMessageToLocalActor( actor, msg, sender, nodeInterface )
        else sendMessageToLocalActor( getBaseByUUID( uuid ), msg, sender, nodeInterface )
      case ActorNodeAllMessage( nodeId, className, msg, sender ) =>
        if ( nodeId == null || nodeId == getNodeId )
          for ( actor <- if ( className == null ) getAllBase else getAllBaseByClassName( className ) )
            sendMessageToLocalActor( actor, msg, sender, nodeInterface )
      case ActorNodeMessageById( nodeId, id, msg, sender ) =>
        if ( nodeId == null || nodeId == getNodeId )
          for ( actor <- if ( id == null ) getAllBase else getAllBaseById( id ) )
            sendMessageToLocalActor( actor, msg, sender, nodeInterface )
      case _ => throw new RuntimeException( "What Happened!" )
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
  def sendMessageNotFound( uuid: UUID, totalSize: Int, chunkSize: Short, address: InetAddress, port: Int ) {
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

  sealed abstract class SendingStatus
  case object NotSent extends SendingStatus
  case object WaitingForReceipt extends SendingStatus
  case object SuccessfullySent extends SendingStatus

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

    def requestReceipt() {
      var doRequest = false
      synchronized {
        if ( status == WaitingForReceipt ) {
          if ( waitRepeatedCount < maxReceiptWaits ) {
            waitRepeatedCount += 1
            resetWaiting()
            doRequest = true
          } else {
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

    def addChunk( index: Int, buf: Array[Byte], offset: Int, length: Int ) {
      synchronized {
        if ( ! chunks.contains( index ) ) {
         Array.copy( buf, offset, bytes, index * chunkSize, length )
         chunks ::= index
        }
        if ( isComplete )
          stopWaiting()
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
    
    def processMessageOnce( address: InetAddress, port: Int, nodeInterface: UDPNodeInterface ) = {
      synchronized {
        if ( message == null ) {
          Node.forSerialization.set( UDPNode.this )
          message = new ObjectInputStream( new ByteArrayInputStream( bytes ) ).readObject() match {
            case message: NodeMessage => message
            case _ => null
          } 
          Node.forSerialization.set( null )
        }
        if ( message != null && ! messageProcessed ) {
          messageProcessed = true
          if ( destination == null )
            processMessage( message, nodeInterface: UDPNodeInterface )
          else if ( destination == getNodeId ) {
            processMessage( message, nodeInterface: UDPNodeInterface )
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
  /**
   * Message sending algorithm:
   * send message ( connection error may be immediate or not ) and mark successfully send chunks
   * wait for response, if no response in time report error on interface to node listener
   * then try another interface. If no more interfaces or out of tries then report message failure to node listener.
   * If actor instance of MessageFailureActor then report message as failed.
   * 
   * MessageChunk => type(B)(1) + uuid(obj) + destCID + total size(int32) + chunk size(int16) + index( start with zero )(int32) + data(B[])
   * MessageReceiptRequest => type(B)(2) + uuid + destCID + total size(int32) + chunk size(int16)
   * MessageReceipt => type(B)(3) + uuid + destCID + total size(int32) + chunk size(int16) + errorCode ( 0=success, 1=failure )(int16)
   * MessageChunksNeeded => type(B)(4) + uuid + destCID + total size(int32) + chunk size(int16) + num ids (int16) + List( id, id, id, id, id ) 
   * MessageChunkRangesNeeded => type(B)(5) + uuid + destCID + total size(int32) + chunk size(int16) + num ids (int16) + List( id - id, id - id, id - id )
   * MessageNoLongerExists => type(B)(6) + uuid + destCID + total size(int32) + chunk size(int16)
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
}

case class UDPNodeInterface( interface: NetworkInterface, address: InetAddress ) extends NodeInterface