package bluemold.actor

import java.io._
import java.lang.ThreadLocal

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

  implicit def getDefaultCluster: Cluster = { // will create it if not set
    if ( defaultCluster == null ) {
      synchronized {
        if ( defaultCluster == null ) {
          defaultCluster = UDPCluster.getCluster( "bluemold", "default" )
        }
      }
    }
    defaultCluster
  }
  
  val forSerialization = new ThreadLocal[Cluster]
}

trait Cluster {
  def startup()
  def shutdown()
  def getAppName: String
  def getGroupName: String

  private[actor] def send( clusterId: ClusterIdentity, message:ClusterMessage, sender: LocalActorRef )

  def send( uuid: UUID, msg: Any, sender: UUID )

  def send( uuid: UUID, msg: Any )( implicit sender: ActorRef )

  def sendAll( clusterId: ClusterIdentity, className: String, msg: Any )( implicit sender: ActorRef )

  def sendAllWithId( clusterId: ClusterIdentity, id: String, msg: Any )( implicit sender: ActorRef )

  def sendAll( clusterId: ClusterIdentity, cl: Class[_], msg: Any )( implicit sender: ActorRef ) { sendAll( clusterId, cl.getName, msg )}

  def sendAll( msg: Any )(implicit sender: ActorRef) { sendAll( null, null: String, msg ) }

  def sendAll( cl: Class[_], msg: Any)(implicit sender: ActorRef) { sendAll( null, cl.getName, msg ) }

  def sendAll(className: String, msg: Any)(implicit sender: ActorRef) { sendAll( null, className, msg ) }

  def sendAll(clusterId: ClusterIdentity, msg: Any)(implicit sender: ActorRef) { sendAll( clusterId, null: String, msg ) }

  def sendAllWithId(id: String, msg: Any)(implicit sender: ActorRef) { sendAllWithId( null, id, msg ) }

  val _clusterId = UUID.getClusterIdentity( getAppName )
  def getClusterId: ClusterIdentity = _clusterId

  val registry = ClusterIdentity.getRegistry( getClusterId )

  def register( registeredActor: BaseRegisteredActor ) { registry.register( registeredActor ) }
  def unRegister( registeredActor: BaseRegisteredActor ) { registry.unRegister( registeredActor ) }

  def getInterfaces: List[NodeInterface]

  def getAllByClassName( className: String ) = registry.getAllByClassName( className )
  def getAll = registry.getAll
  def getAllById( id: String ) = registry.getAllById( id )
  def getByUUID( uuid: UUID ) = registry.getByUUID( uuid )
  def getAllBaseByClassName( className: String ) = registry.getAllBaseByClassName( className )
  def getAllBase = registry.getAllBase
  def getAllBaseById( id: String ) = registry.getAllBaseById( id )
  def getBaseByUUID( uuid: UUID ) = registry.getBaseByUUID( uuid )
  def getCount: Int = registry.getCount
  def getIdCount: Int = registry.getIdCount
  def getClassNameCount: Int = registry.getClassNameCount
  def getIdTotal: Int = registry.getIdTotal
  def getClassNameTotal: Int = registry.getClassNameTotal
}



class ClusterAddress extends Serializable

final case class NodeRoute( target: ClusterIdentity, intermediate: List[ClusterIdentity] )

sealed abstract class ClusterMessage extends Serializable {
  def destination: ClusterIdentity
}

class IdentityClusterMessage extends ClusterMessage {
  var identity: ClusterIdentity = null
  var address: ClusterAddress = null
  def destination = null
} 

case class ActorClusterMessage( recipient: UUID, msg: Any, sender: UUID ) extends ClusterMessage { def destination = recipient.clusterId }
case class ActorClusterAllMessage( clusterId: ClusterIdentity, className: String, msg: Any, sender: UUID ) extends ClusterMessage { def destination = null }
case class ActorClusterMessageById( clusterId: ClusterIdentity, id: String, msg: Any, sender: UUID ) extends ClusterMessage { def destination = null }
case class StopActorClusterMessage( recipient: UUID, sender: UUID ) extends ClusterMessage { def destination = recipient.clusterId }
case class StatusRequestClusterMessage( recipient: UUID, sender: UUID ) extends ClusterMessage { def destination = recipient.clusterId }
case class StatusResponseClusterMessage( recipient: UUID, sender: UUID, stopped: Boolean ) extends ClusterMessage { def destination = recipient.clusterId }

abstract class NodeInterface

trait InterfaceRestrictedActor extends RegisteredActor {
  def isInterfaceAllowed( interface: NodeInterface ): Boolean
}
