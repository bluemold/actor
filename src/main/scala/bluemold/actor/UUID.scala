package bluemold.actor

import scala.util.Random
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap
import java.io.{ObjectStreamException, Serializable}

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
  
  def getNodeStore( appName: String ): Store = Store.getStore( nodeIdentityStore + "_" + appName )

  def getNodeIdentity( appName: String ): NodeIdentity = {
    val store = getNodeStore( appName )
    store.get( nodeIdentityKey ) match {
      case Some( id ) => {
        try {
          parseNodeIdentity( id )
        } catch {
          case _ => generateNodeIdentity( store )
        }
      }
      case None => generateNodeIdentity( store )
    }
  }
  
  private def parseNodeIdentity( identity: String ): NodeIdentity = {
    val idParts = identity.split( UUID.separator )
    if ( idParts.length == 2 ) {
      try {
        new NodeIdentity( idParts(0).toLong, idParts(1).toLong )
      } catch {
        case _ => throw new IllegalStateException() 
      }
    } else throw new IllegalStateException()
  }
  private def generateNodeIdentity( store: Store ): NodeIdentity = {
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

@SerialVersionUID(1L)
final class NodeIdentity( val time: Long, val rand: Long, val path: List[NodeIdentity] ) extends Serializable {
  def this( time: Long, rand: Long ) = this( time, rand, Nil )
  override def toString: String = time.toHexString + UUID.separator + rand.toHexString
  override def equals( obj: Any ): Boolean = obj match {
    case cid: NodeIdentity => cid.time == time && cid.rand == rand
    case cid: TransientSimpleNodeIdentity => cid.time == time && cid.rand == rand
    case cid: TransientNodeIdentity => cid.time == time && cid.rand == rand
    case _ => false
  }
  override def hashCode(): Int = time.hashCode() + rand.hashCode()
  @throws(classOf[ObjectStreamException])
  def writeReplace(): AnyRef = {
    val currentNodeId = Node.forSerialization.get()._nodeId
    if ( this != currentNodeId ) new TransientNodeIdentity( time, rand, ( currentNodeId :: path ) map { new TransientSimpleNodeIdentity( _ ) } )
    else new TransientNodeIdentity( time, rand, Nil )
  }
}

@SerialVersionUID(1L)
final class TransientSimpleNodeIdentity( val time: Long, val rand: Long ) extends Serializable {
  def this( nodeId: NodeIdentity ) = this( nodeId.time, nodeId.rand )
  override def equals( obj: Any ): Boolean = obj match {
    case cid: NodeIdentity => cid.time == time && cid.rand == rand
    case cid: TransientSimpleNodeIdentity => cid.time == time && cid.rand == rand
    case cid: TransientNodeIdentity => cid.time == time && cid.rand == rand
    case _ => false
  }
}

@SerialVersionUID(1L)
final class TransientNodeIdentity( val time: Long, val rand: Long, val path: List[TransientSimpleNodeIdentity] ) extends Serializable {
  override def equals( obj: Any ): Boolean = obj match {
    case cid: NodeIdentity => cid.time == time && cid.rand == rand
    case cid: TransientSimpleNodeIdentity => cid.time == time && cid.rand == rand
    case cid: TransientNodeIdentity => cid.time == time && cid.rand == rand
    case _ => false
  }
  @throws(classOf[ObjectStreamException])
  def readResolve(): AnyRef = {
    val currentNodeId = Node.forSerialization.get()._nodeId
    if ( this == currentNodeId ) currentNodeId
    else if ( path contains currentNodeId ) new NodeIdentity( time, rand,
      path dropWhile { _ != currentNodeId } drop 1
      map { t => new NodeIdentity( t.time, t.rand ) } )
    else new NodeIdentity( time, rand, path map { t => new NodeIdentity( t.time, t.rand ) } )
  }
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