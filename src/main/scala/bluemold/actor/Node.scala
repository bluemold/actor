package bluemold.actor

import java.io._
import java.lang.ThreadLocal
import bluemold.storage.Store

/**
 * Node<br/>
 * Author: Neil Essy<br/>
 * Created: 5/29/11<br/>
 * <p/>
 * [Description]
 */

object Node {
  @volatile var defaultNode: Node = null

  def setDefaultNode( node: Node ) { // able to set only once
    if ( defaultNode == null ) {
      synchronized {
        if ( defaultNode == null ) {
          defaultNode = node
        }
      }
    }
  }

  implicit def getDefaultNode: Node = { // will create it if not set
    if ( defaultNode == null ) {
      synchronized {
        if ( defaultNode == null ) {
          defaultNode = UDPNode.getNode( "bluemold", "default" )
        }
      }
    }
    defaultNode
  }
  
  val forSerialization = new ThreadLocal[Node]
}

trait Node {
  def startup()
  def shutdown()
  def getAppName: String
  def getGroupName: String

  private[actor] def send( nodeId: NodeIdentity, message:NodeMessage, sender: LocalActorRef )

  def send( uuid: UUID, msg: Any, sender: UUID )

  def send( uuid: UUID, msg: Any )( implicit sender: ActorRef )

  def sendAll( nodeId: NodeIdentity, className: String, msg: Any )( implicit sender: ActorRef )

  def sendAllWithId( nodeId: NodeIdentity, id: String, msg: Any )( implicit sender: ActorRef )

  def sendAll( nodeId: NodeIdentity, cl: Class[_], msg: Any )( implicit sender: ActorRef ) { sendAll( nodeId, cl.getName, msg )}

  def sendAll( msg: Any )(implicit sender: ActorRef) { sendAll( null, null: String, msg ) }

  def sendAll( cl: Class[_], msg: Any)(implicit sender: ActorRef) { sendAll( null, cl.getName, msg ) }

  def sendAll(className: String, msg: Any)(implicit sender: ActorRef) { sendAll( null, className, msg ) }

  def sendAll(nodeId: NodeIdentity, msg: Any)(implicit sender: ActorRef) { sendAll( nodeId, null: String, msg ) }

  def sendAllWithId(id: String, msg: Any)(implicit sender: ActorRef) { sendAllWithId( null, id, msg ) }

  val _nodeId = UUID.getNodeIdentity( getAppName )
  def getNodeId: NodeIdentity = _nodeId
  def getStore: Store = UUID.getNodeStore( getAppName )

  val registry = NodeIdentity.getRegistry( getNodeId )

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



class NodeAddress extends Serializable

final case class NodeRoute( target: NodeIdentity, intermediate: List[NodeIdentity] )

sealed abstract class NodeMessage extends Serializable {
  def destination: NodeIdentity
}

case class ActorNodeMessage( recipient: UUID, msg: Any, sender: UUID ) extends NodeMessage { def destination = recipient.nodeId }
case class ActorNodeAllMessage( nodeId: NodeIdentity, className: String, msg: Any, sender: UUID ) extends NodeMessage { def destination = null }
case class ActorNodeMessageById( nodeId: NodeIdentity, id: String, msg: Any, sender: UUID ) extends NodeMessage { def destination = null }
case class StopActorNodeMessage( recipient: UUID, sender: UUID ) extends NodeMessage { def destination = recipient.nodeId }
case class StatusRequestNodeMessage( recipient: UUID, sender: UUID ) extends NodeMessage { def destination = recipient.nodeId }
case class StatusResponseNodeMessage( recipient: UUID, sender: UUID, stopped: Boolean ) extends NodeMessage { def destination = recipient.nodeId }

abstract class NodeInterface

trait InterfaceRestrictedActor extends RegisteredActor {
  def isInterfaceAllowed( interface: NodeInterface ): Boolean
}
