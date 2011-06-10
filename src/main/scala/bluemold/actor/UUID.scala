package bluemold.actor

import bluemold.storage.Store
import scala.util.Random
import java.io.Serializable
import java.lang.IllegalStateException
import java.util.concurrent.ConcurrentHashMap

/**
 * ClusterIdentity<br/>
 * Author: Neil Essy<br/>
 * Created: 5/30/11<br/>
 * <p/>
 * [Description]
 */

object UUID {
  val separator = "-"
  val clusterIdentityStore = "clusterIdentity"
  val clusterIdentityKey = "identity"
  
  def getClusterIdentity( appName: String ): ClusterIdentity = {
    val store = Store.getStore( clusterIdentityStore + "_" + appName )
    store.get( clusterIdentityKey ) match {
      case Some( id ) => {
        try {
          parseClusterIdentity( id )
        } catch {
          case _ => generateClusterIdentity()
        }
      }
      case None => generateClusterIdentity()
    }
  }
  
  def parseUUID( identity: String ): UUID = {
    val idParts = identity.split( UUID.separator )
    if ( idParts.length == 4 ) {
      try {
        new UUID( new ClusterIdentity( idParts(0).toLong, idParts(1).toLong ), idParts(2).toLong, idParts(3).toLong )
      } catch {
        case _ => null 
      }
    } else null
  }
  def parseClusterIdentity( identity: String ): ClusterIdentity = {
    val idParts = identity.split( UUID.separator )
    if ( idParts.length == 2 ) {
      try {
        new ClusterIdentity( idParts(0).toLong, idParts(1).toLong )
      } catch {
        case _ => throw new IllegalStateException() 
      }
    } else throw new IllegalStateException()
  }
  def generateClusterIdentity(): ClusterIdentity = {
    val store = Store.getStore( clusterIdentityStore )
    val clusterId = new ClusterIdentity( System.currentTimeMillis(), Random.nextLong() )
    store.put( clusterIdentityKey, clusterId.toString )
    store.flush()
    clusterId
  }
}

final class ClusterRegistry {
  val byClassName = new ConcurrentHashMap[String,ConcurrentHashMap[LocalActorRef,BaseRegisteredActor]]
  val byId = new ConcurrentHashMap[String,ConcurrentHashMap[LocalActorRef,BaseRegisteredActor]]
  val byUUID = new ConcurrentHashMap[UUID,BaseRegisteredActor]
  
  def register( registeredActor: BaseRegisteredActor ) {
    val className = registeredActor._actor._getClass.getName
    val id = registeredActor._getId
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
        val oldMap = byId.putIfAbsent( className, newMap )
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
      val keys = map.keySet.iterator()
      while ( keys.hasNext )
        ret ::= keys.next() 
    }
    ret
  }
  def getAll(): List[ActorRef] = {
    var ret: List[ActorRef] = Nil
    val iterator = byUUID.values.iterator
    while ( iterator.hasNext )
      ret ::= iterator.next()._localRef
    ret
  }
  def getAllById( id: String ): List[ActorRef] = {
    val map = byId.get( id )
    var ret: List[ActorRef] = Nil
    if ( map != null ) {
      val keys = map.keySet.iterator()
      while ( keys.hasNext )
        ret ::= keys.next() 
    }
    ret
  }
  def getByUUID( uuid: UUID ): ActorRef = {
    val actor = byUUID.get( uuid )
    if ( actor != null )
      actor._localRef
    else null
  }  
}

object ClusterIdentity {
  val registryMap = new ConcurrentHashMap[ClusterIdentity,ClusterRegistry]()
  def getRegistry( clusterId: ClusterIdentity ): ClusterRegistry = {
    val registry = registryMap.get( clusterId )
    if ( registry == null ) {
      val newRegistry = new ClusterRegistry
      val oldRegistry = registryMap.put( clusterId, newRegistry )
      if ( oldRegistry != null ) oldRegistry else newRegistry
    } else registry
  }
}
final class ClusterIdentity( _time: Long, _rand: Long ) extends Serializable {
  def time = _time
  def rand = _rand
  override def toString: String = _time.toHexString + UUID.separator + _rand.toHexString

  override def equals( obj: Any ): Boolean = {
    obj match {
      case cid: ClusterIdentity => cid.time == time && cid.rand == rand
      case _ => false
    }
  }
  override def hashCode(): Int = time.toInt + rand.toInt
}

final class UUID( _clusterId: ClusterIdentity, _time: Long, _rand: Long ) extends Serializable {
  def this( _clusterId: ClusterIdentity ) = this( _clusterId, System.currentTimeMillis(), Random.nextLong() )
  def clusterId = _clusterId
  def time = _time
  def rand = _rand
  override def toString: String = _clusterId.toString + UUID.separator + _time.toHexString + UUID.separator + _rand.toHexString
  override def equals( obj: Any ): Boolean = {
    obj match {
      case uuid: UUID => uuid.clusterId == clusterId && uuid.time == time && uuid.rand == rand
      case _ => false
    }
  }
  override def hashCode(): Int = clusterId.hashCode() + time.toInt + rand.toInt
}