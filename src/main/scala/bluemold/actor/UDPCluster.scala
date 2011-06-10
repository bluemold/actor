package bluemold.actor

import collection.immutable.HashMap
import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}
import java.net._
import java.util.concurrent.ConcurrentHashMap
import annotation.tailrec

/**
 * UDPCluster<br/>
 * Author: Neil Essy<br/>
 * Created: 6/7/11<br/>
 * <p/>
 * [Description]
 */

private object UDPCluster {
  val broadcastPort = 9900
  val minPort = 9901
  val maxPort = 9999
}

case class UDPAddress( address: InetAddress, port: Int )

final class UDPCluster( appName: String, groupName: String ) extends Cluster {
  import UDPCluster._

  val sockets = getSockets(  NetworkInterface.getNetworkInterfaces )

  private def getSockets( interfaces: java.util.Enumeration[NetworkInterface] ): HashMap[InterfaceAddress,(DatagramSocket,DatagramSocket,InetAddress)] = {
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

  def getAppName: String = appName
  def getGroupName: String = groupName

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
  }

  def shutdown() {
  }

  private def send( clusterId: ClusterIdentity, out: ByteArrayOutputStream ) {
    getDestination( clusterId ) match {
      case Some((udpAddress,socket)) => {
        val bytes = out.toByteArray
//        println( "Send: From: " + socket.getLocalAddress + ":" + socket.getLocalPort + " To: " + udpAddress.address + ":" + udpAddress.port )
        val packet = new DatagramPacket(bytes,bytes.length,udpAddress.address,udpAddress.port)
        socket.send(packet)
      }
      case None => {
        val bytes = out.toByteArray
        for ( (socket,broadcast,bAddress) <- sockets.values ) {
//          println( "MSend: From: " + socket.getLocalAddress + ":" + socket.getLocalPort + " To: " + bAddress + ":" + broadcastPort )
          val packet = new DatagramPacket(bytes,bytes.length,bAddress,broadcastPort)
          socket.send(packet)
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
  
  class Receiver( socket: DatagramSocket ) extends Runnable {
    def run() {
      val buffer = new Array[Byte](16384)
      val packet = new DatagramPacket(buffer,buffer.length)
      while ( true ) {
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
              for ( actor <- getAll() )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterMessage( uuid, msg, senderUUID ) => { 
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              val actor = getByUUID( uuid )
              if ( actor != null )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            }
            case ActorClusterAllMessage( null, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAll() )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterAllMessage( null, className, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAllByClassName( className ) )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            case ActorClusterAllMessage( clusterId, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAll() )
                  actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterAllMessage( clusterId, className, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAllByClassName( className ) )
                  actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            case ActorClusterMessageById( null, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAll() )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this)) 
            case ActorClusterMessageById( null, id, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              for ( actor <- getAllById( id ) )
                actor.!(msg)(new RemoteActorRef(senderUUID,UDPCluster.this))
            case ActorClusterMessageById( clusterId, null, msg, senderUUID ) =>
              updateClusterAddressMap( senderUUID.clusterId, packet.getAddress, packet.getPort )
              if ( clusterId == getClusterId )
                for ( actor <- getAll() )
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
}
