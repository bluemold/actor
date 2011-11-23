package bluemold.actor

import bluemold.storage.Store
import scala.util.Random
import java.io.Serializable
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap

/**
 * NodeIdentity<br/>
 * Author: Neil Essy<br/>
 * Created: 5/30/11<br/>
 * <p/>
 * [Description]
 */

object UUID {
  val separator = "-"
  val nodeIdentityStore = "nodeIdentity"
  val nodeIdentityKey = "identity"
  
  def getNodeIdentity( appName: String ): NodeIdentity = {
    val store = Store.getStore( nodeIdentityStore + "_" + appName )
    store.get( nodeIdentityKey ) match {
      case Some( id ) => {
        try {
          parseNodeIdentity( id )
        } catch {
          case _ => generateNodeIdentity()
        }
      }
      case None => generateNodeIdentity()
    }
  }
  
  def parseUUID( identity: String ): UUID = {
    val idParts = identity.split( UUID.separator )
    if ( idParts.length == 4 ) {
      try {
        new UUID( new NodeIdentity( idParts(0).toLong, idParts(1).toLong ), idParts(2).toLong, idParts(3).toLong )
      } catch {
        case _ => null 
      }
    } else null
  }
  def parseNodeIdentity( identity: String ): NodeIdentity = {
    val idParts = identity.split( UUID.separator )
    if ( idParts.length == 2 ) {
      try {
        new NodeIdentity( idParts(0).toLong, idParts(1).toLong )
      } catch {
        case _ => throw new IllegalStateException() 
      }
    } else throw new IllegalStateException()
  }
  def generateNodeIdentity(): NodeIdentity = {
    val store = Store.getStore( nodeIdentityStore )
    val nodeId = new NodeIdentity( System.currentTimeMillis(), Random.nextLong() )
    store.put( nodeIdentityKey, nodeId.toString )
    store.flush()
    nodeId
  }
}

final class NodeRegistry {
  val byClassName = new ConcurrentHashMap[String,ConcurrentHashMap[LocalActorRef,BaseRegisteredActor]]
  val byId = new ConcurrentHashMap[String,ConcurrentHashMap[LocalActorRef,BaseRegisteredActor]]
  val byUUID = new ConcurrentHashMap[UUID,BaseRegisteredActor]
  
  def getCount: Int = byUUID.size()
  def getIdCount: Int = byId.size()
  def getClassNameCount: Int = byClassName.size()
  def getIdTotal: Int = {
    var total = 0
    val elements = byId.elements()
    while ( elements.hasMoreElements )
      total += ( elements.nextElement().size() )
    total
  }
  def getClassNameTotal: Int = {
    var total = 0
    val elements = byClassName.elements()
    while ( elements.hasMoreElements )
      total += ( elements.nextElement().size() )
    total
  }

  def register( registeredActor: BaseRegisteredActor ) {
    val className = registeredActor._actor._getClass.getName
    val id = registeredActor._actor._getId
    val uuid = registeredActor._getUUID

    ( { val map = byClassName.get( className )
      if ( map == null ) {
        val newMap = new ConcurrentHashMap[LocalActorRef,BaseRegisteredActor]
        val oldMap = byClassName.putIfAbsent( className, newMap )
        if ( oldMap != null ) oldMap else newMap
      } else map
    } ).put( registeredActor._localRef, registeredActor )

    ( { val map = byId.get( id )
      if ( map == null ) {
        val newMap = new ConcurrentHashMap[LocalActorRef,BaseRegisteredActor]
        val oldMap = byId.putIfAbsent( id, newMap )
        if ( oldMap != null ) oldMap else newMap
      } else map
    } ).put( registeredActor._localRef, registeredActor )

    byUUID.put( uuid, registeredActor )
  }
  def unRegister( registeredActor: BaseRegisteredActor ) {
    val className = registeredActor._actor._getClass.getName
    val id = registeredActor._getId
    val uuid = registeredActor._getUUID

    { val map = byClassName.get( className )
      if ( map != null )
        map.remove( registeredActor._localRef )
    }

    { val map = byId.get( id )
      if ( map != null )
        map.remove( registeredActor._localRef )
    }

    byUUID.remove( uuid )
  }
  def getAllByClassName( className: String ): List[ActorRef] = {
    val map = byClassName.get( className )
    var ret: List[ActorRef] = Nil
    if ( map != null ) {
      val keys = map.keys()
      while ( keys.hasMoreElements )
        ret ::= keys.nextElement() 
    }
    ret
  }
  def getAll: List[ActorRef] = {
    var ret: List[ActorRef] = Nil
    val elements = byUUID.elements()
    while ( elements.hasMoreElements )
      ret ::= elements.nextElement()._localRef
    ret
  }
  def getAllById( id: String ): List[ActorRef] = {
    val map = byId.get( id )
    var ret: List[ActorRef] = Nil
    if ( map != null ) {
      val keys = map.keys()
      while ( keys.hasMoreElements )
        ret ::= keys.nextElement() 
    }
    ret
  }
  def getByUUID( uuid: UUID ): ActorRef = {
    val actor = byUUID.get( uuid )
    if ( actor != null )
      actor._localRef
    else null
  }  

  def getAllBaseByClassName( className: String ): List[BaseRegisteredActor] = {
    val map = byClassName.get( className )
    var ret: List[BaseRegisteredActor] = Nil
    if ( map != null ) {
      val elements = map.elements()
      while ( elements.hasMoreElements )
        ret ::= elements.nextElement()
    }
    ret
  }
  def getAllBase: List[BaseRegisteredActor] = {
    var ret: List[BaseRegisteredActor] = Nil
    val elements = byUUID.elements()
    while ( elements.hasMoreElements )
      ret ::= elements.nextElement()
    ret
  }
  def getAllBaseById( id: String ): List[BaseRegisteredActor] = {
    val map = byId.get( id )
    var ret: List[BaseRegisteredActor] = Nil
    if ( map != null ) {
      val elements = map.elements()
      while ( elements.hasMoreElements )
        ret ::= elements.nextElement()
    }
    ret
  }
  def getBaseByUUID( uuid: UUID ): BaseRegisteredActor = byUUID.get( uuid )
}

object NodeIdentity {
  val registryMap = new ConcurrentHashMap[NodeIdentity,NodeRegistry]()
  def getRegistry( nodeId: NodeIdentity ): NodeRegistry = {
    val registry = registryMap.get( nodeId )
    if ( registry == null ) {
      val newRegistry = new NodeRegistry
      val oldRegistry = registryMap.put( nodeId, newRegistry )
      if ( oldRegistry != null ) oldRegistry else newRegistry
    } else registry
  }
}
final class NodeIdentity( _time: Long, _rand: Long ) extends Serializable {
  def time = _time
  def rand = _rand
  override def toString: String = _time.toHexString + UUID.separator + _rand.toHexString

  override def equals( obj: Any ): Boolean = {
    obj match {
      case cid: NodeIdentity => cid.time == time && cid.rand == rand
      case _ => false
    }
  }
  override def hashCode(): Int = time.hashCode() + rand.hashCode()
}

final class UUID( _nodeId: NodeIdentity, _time: Long, _rand: Long ) extends Serializable {
  def this( _nodeId: NodeIdentity ) = this( _nodeId, System.currentTimeMillis(), Random.nextLong() )
  def nodeId = _nodeId
  def time = _time
  def rand = _rand
  override def toString: String = _nodeId.toString + UUID.separator + _time.toHexString + UUID.separator + _rand.toHexString
  override def equals( obj: Any ): Boolean = obj match {
    case uuid: UUID => uuid.nodeId == nodeId && uuid.time == time && uuid.rand == rand
    case _ => false
  }
  override def hashCode(): Int = nodeId.hashCode() + time.hashCode() + rand.hashCode()
}