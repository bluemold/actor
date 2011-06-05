package bluemold.cluster

import bluemold.storage.Store
import util.Random
import java.io.Serializable

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
  val clusterIdentity: ClusterIdentity = getClusterIdentity
  
  def getClusterIdentity: ClusterIdentity = {
    val store = Store.getStore( clusterIdentityStore )
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
        case _ => null 
      }
    } else null
  }
  def generateClusterIdentity(): ClusterIdentity = {
    val store = Store.getStore( clusterIdentityStore )
    val clusterId = new ClusterIdentity( System.currentTimeMillis(), Random.nextLong() )
    store.put( clusterIdentityKey, clusterId.toString )
    store.flush()
    clusterId
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
  def this() = this( UUID.getClusterIdentity, System.currentTimeMillis(), Random.nextLong() )
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