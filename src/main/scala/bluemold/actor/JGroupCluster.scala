package bluemold.actor

import bluemold.concurrent.NonLockingHashMap
import java.io._
import java.util.concurrent.ConcurrentLinkedQueue
import org.jgroups._

/**
 * JGroupCluster<br/>
 * Author: Neil Essy<br/>
 * Created: 6/7/11<br/>
 * <p/>
 * [Description]
 */

final class JGroupClusterAddress extends ClusterAddress {
  var address: Address = null
}

final class JGroupCluster( appName: String, groupName: String ) extends Cluster {
  val channel: JChannel = new JChannel()

  val addressToId: NonLockingHashMap[Address,ClusterIdentity] = new NonLockingHashMap[Address,ClusterIdentity]
  val idToAddresses: NonLockingHashMap[ClusterIdentity,List[Address]] = new NonLockingHashMap[ClusterIdentity, List[Address]]

  @volatile var view: View = null
  @volatile var blocked: Boolean = false
  @volatile var state: Array[Byte] = new Array[Byte](0)

  val sendQueue = new ConcurrentLinkedQueue[ClusterMessage]
  
  def startup() {
    if ( ! channel.isConnected ) {
      channel.connect( groupName )
    }
  }
  def shutdown() {
    if ( channel.isConnected ) {
      channel.disconnect()
    }
  }

  def getAppName: String = appName

  def getGroupName: String = groupName

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
          val localActorRef = getByUUID( recipient )
          if ( localActorRef != null )
            localActorRef.!(msg)(new RemoteActorRef(sender,JGroupCluster.this)) 
        }
        case ActorClusterAllMessage( clusterId, className, msg, sender ) => {
          if ( clusterId == null || getClusterId == clusterId ) {
            if ( className == null )
              getAll().foreach( ( localActorRef ) => localActorRef.!(msg)(new RemoteActorRef(sender,JGroupCluster.this)) )
            else
              getAllByClassName( className ).foreach( ( localActorRef ) => localActorRef.!(msg)(new RemoteActorRef(sender,JGroupCluster.this)) )
          }
        }
        case ActorClusterMessageById( clusterId, id, msg, sender ) => {
          if ( clusterId == null || getClusterId == clusterId ) {
            if ( id == null )
              getAll().foreach( ( localActorRef ) => localActorRef.!(msg)(new RemoteActorRef(sender,JGroupCluster.this)) )
            else
              getAllById( id ).foreach( ( localActorRef ) => localActorRef.!(msg)(new RemoteActorRef(sender,JGroupCluster.this)) )
          }
        }
        case _ => // do nothing
      }
    }

    def getState: Array[Byte] = state
    def setState( p1: Array[Byte] ) { state = p1 }
  }


  private[actor] def send(clusterId: ClusterIdentity, message: ClusterMessage) {
    if ( ! blocked ) {
      val out = new ByteArrayOutputStream
      val objectOut = new ObjectOutputStream( out )
      objectOut.writeObject( message )
      val destination = getDestination( clusterId )
      channel.send( destination, null, message )
    }
  }

  def send( uuid: UUID, msg: Any, sender: UUID ) {
    val clusterMessage = ActorClusterMessage( uuid, msg, sender )
    if ( ! blocked ) {
      val out = new ByteArrayOutputStream
      val objectOut = new ObjectOutputStream( out )
      objectOut.writeObject( clusterMessage )
      val destination = getDestination( uuid.clusterId )
      channel.send( destination, null, clusterMessage )
    } else sendQueue.add( clusterMessage )
  }

  def send( uuid: UUID, msg: Any)( implicit sender: ActorRef ) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null )
      send( uuid, msg, localActorRef._getUUID )
  }

  def sendAll(clusterId: ClusterIdentity, className: String, msg: Any)(implicit sender: ActorRef) {
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null ) {
      val clusterMessage = ActorClusterAllMessage( clusterId, className, msg, localActorRef._getUUID )
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
    val localActorRef = if ( sender.isInstanceOf[LocalActorRef] ) sender.asInstanceOf[LocalActorRef] else null
    if ( localActorRef != null ) {
      val clusterMessage = ActorClusterMessageById( clusterId, id, msg, localActorRef._getUUID )
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

