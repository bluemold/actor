package bluemold.actor

import collection.immutable.HashMap
import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.net._
import java.util.concurrent.ConcurrentHashMap
import annotation.tailrec
import bluemold.concurrent.{AtomicInteger, AtomicBoolean}

/**
 * UDPCluster<br/>
 * Author: Neil Essy<br/>
 * Created: 6/7/11<br/>
 * <p/>
 * [Description]
 */

case class LocalClusterIdentity( appName: String, groupName: String )

object UDPCluster {
  private[actor] val broadcastPort = 9900
  private[actor] val minPort = 9901
  private[actor] val maxPort = 9999

  val clusters = new ConcurrentHashMap[LocalClusterIdentity,UDPCluster]

  def getCluster( appName: String, groupName: String ) = {
    val localId = LocalClusterIdentity( appName, groupName )
    val cluster = clusters.get( localId )
    if ( cluster == null ) {
      val cluster = new UDPCluster( localId )
      val oldCluster = clusters.putIfAbsent( localId, cluster )
      if ( oldCluster == null ) { cluster.startup(); cluster }
      else oldCluster
    } else cluster
  }
}

case class UDPAddress( address: InetAddress, port: Int )

final class UDPCluster( localId: LocalClusterIdentity ) extends Cluster {
  import UDPCluster._

  val done = new AtomicBoolean()

  val numSentBuckets = 4
  val sent = Array.fill( numSentBuckets ) { new ConcurrentHashMap[UUID,ClusterMessage] }
  val sentIndex = new AtomicInteger()
  val sentDelay = 10000

  val numReceivedBuckets = 4
  val received = Array.fill( numSentBuckets ) { new ConcurrentHashMap[UUID,ClusterMessage] }
  val receivedIndex = new AtomicInteger()
  val receivedDelay = 60000


  @volatile var sockets: HashMap[InterfaceAddress,(DatagramSocket,DatagramSocket,InetAddress)] = _

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

  private def getSockets0( interfaces: java.util.Enumeration[NetworkInterface] ): HashMap[InterfaceAddress,(DatagramSocket,DatagramSocket,InetAddress)] = {
    var map = new HashMap[InterfaceAddress,(DatagramSocket,DatagramSocket,InetAddress)]()
    while ( interfaces.hasMoreElements ) {
      val interface = interfaces.nextElement()
      if ( ! interface.isLoopback ) {
        val addresses = interface.getInterfaceAddresses.iterator() 
        while ( addresses.hasNext ) {
          val address = addresses.next()
          val socket = getSocket( minPort, address )
          if ( socket != null ) {
            val broadcastSocket = new DatagramSocket( null: SocketAddress )
            broadcastSocket.setReuseAddress( true )
            broadcastSocket.bind( new InetSocketAddress( address.getAddress, broadcastPort ) )

            val broadcastReceiver = new Thread( new Receiver( broadcastSocket ) )
            broadcastReceiver.setDaemon( true )
            broadcastReceiver.start()

            val socketReceiver = new Thread( new Receiver( socket ) )
            socketReceiver.setDaemon( true )
            socketReceiver.start()

            map += ((address,((socket,broadcastSocket,address.getBroadcast))))
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

  val addressToId: ConcurrentHashMap[UDPAddress,ClusterIdentity] = new ConcurrentHashMap[UDPAddress,ClusterIdentity]
  val idToAddresses: ConcurrentHashMap[ClusterIdentity,List[UDPAddress]] = new ConcurrentHashMap[ClusterIdentity, List[UDPAddress]]

  def getDestination( clusterId: ClusterIdentity ): Option[(UDPAddress,DatagramSocket)] = {
    val target: UDPAddress = if ( clusterId != null ) {
      val addys = idToAddresses.get( clusterId )
      addys match {
        case head :: tail => head
        case Nil => null
        case null => null
      }
    } else null
    
    if ( target != null ) {
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
                case Some( ( aSocket, aBroadcast, bAddress ) ) => socket = aSocket
                case None =>
              }
            }
          }
        }
      }
      if ( socket != null ) Some((target,socket)) else None
    } else None
  } 

  def startup() {
    getSockets
    val sentCleaner = new Thread( new SentCleaner() )
    sentCleaner.setDaemon( true )
    sentCleaner.start()
    val receivedCleaner = new Thread( new ReceivedCleaner() )
    receivedCleaner.setDaemon( true )
    receivedCleaner.start()
  }

  def shutdown() {
    done.set( true )
    clusters.remove( localId, this )
    for ( (socket,broadcast,bAddress) <- getSockets.values ) {
      try {
        socket.close()
      } catch { case t: Throwable => t.printStackTrace() }
      try {
        broadcast.close()
      } catch { case t: Throwable => t.printStackTrace() }
    }
  }

  private def send( clusterId: ClusterIdentity, out: ByteArrayOutputStream ) {
    getDestination( clusterId ) match {
      case Some((udpAddress,socket)) => {
        val bytes = out.toByteArray
//        println( "Send: From: " + socket.getLocalAddress + ":" + socket.getLocalPort + " To: " + udpAddress.address + ":" + udpAddress.port )
        val packet = new DatagramPacket(bytes,bytes.length,udpAddress.address,udpAddress.port)
        try {
          socket.send(packet)
        } catch { case t: Throwable => t.printStackTrace() }
      }
      case None => {
        val bytes = out.toByteArray
        for ( (socket,broadcast,bAddress) <- getSockets.values ) {
//          println( "MSend: From: " + socket.getLocalAddress + ":" + socket.getLocalPort + " To: " + bAddress + ":" + broadcastPort )
          val packet = new DatagramPacket(bytes,bytes.length,bAddress,broadcastPort)
          try {
            socket.send(packet)
          } catch { case t: Throwable => t.printStackTrace() }
        }
      }
    }
  } 

  
  
  def send( clusterId: ClusterIdentity, message:ClusterMessage ) {
    val out = new ByteArrayOutputStream
    val objectOut = new ObjectOutputStream( out )
    objectOut.writeObject( message )
    objectOut.flush()
    send( clusterId, out )
  }

  def send(uuid: UUID, msg: Any, sender: UUID ) {
    send( uuid.clusterId, ActorClusterMessage( uuid, msg, sender ) )
  }

  def send(uuid: UUID, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( uuid, msg, localActorRef._getUUID )
  }

  def sendAll(clusterId: ClusterIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( clusterId, ActorClusterAllMessage( clusterId, className, msg, localActorRef._getUUID ) )
  }

  def sendAllWithId(clusterId: ClusterIdentity, id: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( clusterId, ActorClusterMessageById( clusterId, id, msg, localActorRef._getUUID ) )
  }
  
  def updateClusterAddressMap( clusterId: ClusterIdentity, address: InetAddress, port: Int ) {
    updateClusterAddressMap0( clusterId, UDPAddress( address, port ) )
  }

  @tailrec
  def updateClusterAddressMap0( clusterId: ClusterIdentity, udpAddress: UDPAddress ) {
    idToAddresses.get( clusterId ) match {
      case addys: List[UDPAddress] =>
        if ( ! addys.contains( udpAddress ) && ! idToAddresses.replace( clusterId, udpAddress :: addys, addys ) )
          updateClusterAddressMap0( clusterId, udpAddress )
      case null =>
        if ( idToAddresses.putIfAbsent( clusterId, udpAddress :: Nil ) != null )
          updateClusterAddressMap0( clusterId, udpAddress )
    }
    
  }
  
  final class Receiver( socket: DatagramSocket ) extends Runnable {
    def run() {
      val buffer = new Array[Byte](16384)
      val packet = new DatagramPacket(buffer,buffer.length)
      while ( ! done.get() ) {
        try {
          socket.receive( packet )
          val in = new ByteArrayInputStream( packet.getData, packet.getOffset, packet.getLength )
          val objIn = new ObjectInputStream( in )
          val obj = objIn.readObject()
          
          obj match {
            case StopActorClusterMessage( recipient: UUID, sender: UUID ) =>
              updateClusterAddressMap( sender.clusterId, packet.getAddress, packet.getPort )
              val actor = getByUUID( recipient )
              if ( actor != null )
                actor.stop()
            case StatusRequestClusterMessage( recipient: UUID, sender: UUID ) =>
              updateClusterAddressMap( sender.clusterId, packet.getAddress, packet.getPort )
              val actor = getByUUID( recipient )
              if ( actor != null )
                send( sender.clusterId, StatusResponseClusterMessage( sender, recipient, ! actor.isActive ) )
            case StatusResponseClusterMessage( recipient: UUID, sender: UUID, stopped: Boolean ) =>
              updateClusterAddressMap( sender.clusterId, packet.getAddress, packet.getPort )
              val actor = getByUUID( recipient )
              if ( actor != null )
                actor.!(stopped)(new RemoteActorRef(sender,UDPCluster.this)) 
            case ActorClusterMessage( null, msg, senderUUID ) => 
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAll )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterMessage( uuid, msg, senderUUID ) => { 
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              val actor = getByUUID( uuid )
              if ( actor != null )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            }
            case ActorClusterAllMessage( null, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAll )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterAllMessage( null, className, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAllByClassName( className ) )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            case ActorClusterAllMessage( clusterId, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAll )
                  actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterAllMessage( clusterId, className, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAllByClassName( className ) )
                  actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            case ActorClusterMessageById( null, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAll )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterMessageById( null, id, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAllById( id ) )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            case ActorClusterMessageById( clusterId, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAll )
                  actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterMessageById( clusterId, id, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAllById( id ) )
                  actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
          }
        } catch { case t: Throwable => t.printStackTrace() } // log and try again
      }
    }
  }

  /**
   * Message sending algorithm:
   * send message ( connection error may be immediate or not ) and mark successfully send chunks
   * wait for response, if no response in time report error on interface to cluster listener
   * then try another interface. If no more interfaces or out of tries then report message failure to cluster listener.
   * If actor instance of MessageFailureActor then report message as failed.
   * 
   * MessageChunk => type + uuid + total chunks + index( start with zero ) + data
   * MessageSingleton => type + uuid + data
   * MessageReceiptRequest => type + uuid + total chunks
   * MessageReceipt => type + uuid + errorCode ( 0=success ) + chunks received ( if reported received != expected received then error )
   * MessageChunksNeeded => type + uuid + List( id, id, id, id, id ) 
   * MessageChunkRangesNeeded => type + uuid + List( id - id, id - id, id - id )
   * 
   * Receiver triggers MessageReceipt upon receiving all chunks
   * If sender does not hear back after a set time he sends a recipt request
   * Receiver replys to a recipt request with a recipt or chunks needed response.
   * The chunks needed response is sent even when the reciever never heard any of the chunks.
   * 
   * If the receiver has not gotten all the chunks by a set time they respond with a chunks needed.
   * If the receiver does not receive all the chunks by a set time then it is simply forgotten.
   */
  final class SentCleaner() extends Runnable {
    def run() {
      while ( ! done.get() ) {
        synchronized { wait( sentDelay ) }
        val currentBucket = sentIndex.modIncrementAndGet( numSentBuckets )
        // TODO
      }
    }
  }

  final class ReceivedCleaner() extends Runnable {
    def run() {
      while ( ! done.get() ) {
        synchronized { wait( receivedDelay ) }
        val currentBucket = receivedIndex.modIncrementAndGet( numReceivedBuckets )
        // TODO
      }
    }
  }
}
