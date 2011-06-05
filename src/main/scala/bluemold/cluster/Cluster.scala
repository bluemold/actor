package bluemold.cluster

import org.jgroups._
import bluemold.concurrent.NonLockingHashMap
import java.io._
import java.util.concurrent.ConcurrentLinkedQueue
import bluemold.actor.{ActorRef, BaseActor, RegisteredActorRef, LocalActorRef}

/**
 * Cluster<br/>
 * Author: Neil Essy<br/>
 * Created: 5/29/11<br/>
 * <p/>
 * [Description]
 */

object Cluster {
  @volatile var defaultCluster: Cluster = null

  def setDefaultCluster( cluster: Cluster ) { // able to set only once
    if ( defaultCluster == null ) {
      synchronized {
        if ( defaultCluster == null ) {
          defaultCluster = cluster
        }
      }
    }
  }

  def getDefaultCluster: Cluster = { // will create it if not set
    if ( defaultCluster == null ) {
      synchronized {
        if ( defaultCluster == null ) {
          val cluster = new JGroupCluster( "default" )
          cluster.startup()
          defaultCluster = cluster
        }
      }
    }
    defaultCluster
  }
}

trait Cluster {
  def startup()
  def shutdown()
  def getName: String

  def send( uuid: UUID, msg: Any )( implicit sender: ActorRef )

  def sendAll( clusterId: ClusterIdentity, className: String, msg: Any )( implicit sender: ActorRef )

  def sendAllWithId( clusterId: ClusterIdentity, id: String, msg: Any )( implicit sender: ActorRef )

  def sendAll( msg: Any )(implicit sender: ActorRef) { sendAll( null, null, msg ) }

  def sendAll(className: String, msg: Any)(implicit sender: ActorRef) { sendAll( null, className, msg ) }

  def sendAll(clusterId: ClusterIdentity, msg: Any)(implicit sender: ActorRef) { sendAll( clusterId, null, msg ) }

  def sendAllWithId(id: String, msg: Any)(implicit sender: ActorRef) { sendAllWithId( null, id, msg ) }
}

class JGroupCluster( name: String ) extends Cluster {
  val channel: JChannel = new JChannel()

  val addressToId: NonLockingHashMap[Address,ClusterIdentity] = new NonLockingHashMap[Address,ClusterIdentity]
  val idToAddresses: NonLockingHashMap[ClusterIdentity,List[Address]] = new NonLockingHashMap[ClusterIdentity, List[Address]]

  @volatile var view: View = null
  @volatile var blocked: Boolean = false
  @volatile var state: Array[Byte] = new Array[Byte](0)

  val sendQueue = new ConcurrentLinkedQueue[ClusterMessage]
  
  def startup() {
    if ( ! channel.isConnected ) {
      channel.connect( name )
    }
  }
  def shutdown() {
    if ( channel.isConnected ) {
      channel.disconnect()
    }
  }
  def getName: String = name

  def getDestination( clusterId: ClusterIdentity ): Address = {
    if ( clusterId != null ) {
      val addys = idToAddresses.get( clusterId )
      addys match {
        case Some( head :: tail ) => head
        case Some( Nil ) => null
        case None => null
      }
    } else null
  } 

  class Receiver extends ExtendedMembershipListener with MessageListener {

    def viewAccepted( p1: View ) { view = p1 }
    def suspect( p1: Address ) {
      // todo
    }
    def block() { blocked = true }
    def unblock() { blocked = false }

    def receive( p1: Message ) {
      val in = new ByteArrayInputStream( p1.getRawBuffer, p1.getOffset, p1.getLength )
      val objectIn = new ObjectInputStream( in )
      val msg = objectIn.readObject()
      msg match {
        case msg: IdentityClusterMessage => // do nothing
        case ActorClusterMessage( recipient, msg, sender ) => {
          val localActorRef = BaseActor.getByUUID( recipient )
          if ( localActorRef != null )
            localActorRef.!(msg)(sender) 
        }
        case ActorClusterAllMessage( clusterId, className, msg, sender ) => {
          if ( clusterId == null || UUID.getClusterIdentity == clusterId ) {
            if ( className == null )
              BaseActor.getAll().foreach( ( localActorRef ) => localActorRef.!(msg)(sender) )
            else
              BaseActor.getAllByClassName( className ).foreach( ( localActorRef ) => localActorRef.!(msg)(sender) )
          }
        }
        case ActorClusterMessageById( clusterId, id, msg, sender ) => {
          if ( clusterId == null || UUID.getClusterIdentity == clusterId ) {
            if ( id == null )
              BaseActor.getAll().foreach( ( localActorRef ) => localActorRef.!(msg)(sender) )
            else
              BaseActor.getAllById( id ).foreach( ( localActorRef ) => localActorRef.!(msg)(sender) )
          }
        }
        case _ => // do nothing
      }
    }

    def getState: Array[Byte] = state
    def setState( p1: Array[Byte] ) { state = p1 }
  }

  def send( uuid: UUID, msg: Any)( implicit sender: ActorRef ) {
    val registeredActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef].registeredActorRef else null
    if ( registeredActorRef != null ) {
      val clusterMessage = ActorClusterMessage( uuid, msg, registeredActorRef )
      if ( ! blocked ) {
        val out = new ByteArrayOutputStream
        val objectOut = new ObjectOutputStream( out )
        objectOut.writeObject( clusterMessage )
        val destination = getDestination( uuid.clusterId )
        channel.send( destination, null, clusterMessage )
      } else sendQueue.add( clusterMessage )
    }
  }

  def sendAll(clusterId: ClusterIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val registeredActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef].registeredActorRef else null
    if ( registeredActorRef != null ) {
      val clusterMessage = ActorClusterAllMessage( clusterId, className, msg, registeredActorRef )
      if ( ! blocked ) {
        val out = new ByteArrayOutputStream
        val objectOut = new ObjectOutputStream( out )
        objectOut.writeObject( clusterMessage )
        val destination = getDestination( clusterId )
        channel.send( destination, null, clusterMessage )
      } else sendQueue.add( clusterMessage )
    }
  }

  def sendAllWithId(clusterId: ClusterIdentity, id: String, msg: Any)(implicit sender: ActorRef) {
    val registeredActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef].registeredActorRef else null
    if ( registeredActorRef != null ) {
      val clusterMessage = ActorClusterMessageById( clusterId, id, msg, registeredActorRef )
      if ( ! blocked ) {
        val out = new ByteArrayOutputStream
        val objectOut = new ObjectOutputStream( out )
        objectOut.writeObject( clusterMessage )
        val destination = getDestination( clusterId )
        channel.send( destination, null, clusterMessage )
      } else sendQueue.add( clusterMessage )
    }
  }
}

class ClusterAddress extends Serializable

class JGroupClusterAddress extends ClusterAddress {
  var address: Address = null
}

class ClusterMessage extends Serializable

class IdentityClusterMessage extends ClusterMessage {
  var identity: ClusterIdentity = null
  var address: ClusterAddress = null
} 

case class ActorClusterMessage( recipient: UUID, msg: Any, sender: RegisteredActorRef ) extends ClusterMessage
case class ActorClusterAllMessage( clusterId: ClusterIdentity, className: String, msg: Any, sender: RegisteredActorRef ) extends ClusterMessage
case class ActorClusterMessageById( clusterId: ClusterIdentity, id: String, msg: Any, sender: RegisteredActorRef ) extends ClusterMessage
