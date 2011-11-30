package bluemold.storage

import bluemold.concurrent.{AtomicBoolean, NonLockingHashMap}
import java.util.Properties
import java.io.{File, FileInputStream, FileOutputStream}

/**
 * Store<br/>
 * Author: Neil Essy<br/>
 * Created: 5/29/11<br/>
 * <p/>
 * [Description]
 */

object Store {
  @volatile var defaultStoreFactory: StoreFactory = new FileStoreFactory() 
  def getDefaultStoreFactory = defaultStoreFactory
  def setDefaultStoreFactory( storeFactory: StoreFactory ) { defaultStoreFactory = storeFactory }
  def getStore( name: String ): Store = defaultStoreFactory.getStore( name )
}

trait Store {
  def get( key: String ): Option[String]
  def put( key: String, value: String )
  def remove( key: String )
  def isChanged: Boolean
  def flush()
  def getFileContainer( name: String ): File
}

trait StoreFactory {
  def getStore( name: String ): Store
}

class FileStoreFactory extends StoreFactory {
  val fileStores: NonLockingHashMap[String,FileStore] = new NonLockingHashMap[String,FileStore]()  
  def getStore( name: String ): Store = {
    fileStores.getOrElseUpdate( name, new FileStore( name ) )
  }
}

class FileStore( name: String ) extends Store {
  val changed: AtomicBoolean = AtomicBoolean.create()
  val map: NonLockingHashMap[String,String] = new NonLockingHashMap[String, String]()
  val parentPath = "./storage"
  val path = name + ".store"

  try {
    val props = new Properties()
    val parentDir = new File( parentPath )
    if ( ! parentDir.exists() )
      parentDir.mkdirs()
    val file = new File( parentDir, path )
    if ( file.exists() ) {
      val fileStream = new FileInputStream( file )
      props.loadFromXML( fileStream )
      val keys = props.keys()
      while ( keys.hasMoreElements ) {
        val key = keys.nextElement().asInstanceOf[String]
        map.put( key, props.get( key ).asInstanceOf[String] )
      }
    }
  } catch {
    case t: Throwable => throw new RuntimeException( "Failed to load: " + name, t )
  }
  
  def get(key: String): Option[String] = map.get( key )

  def put(key: String, value: String ) {
    map.put( key, value )
    changed.set( true )
  }

  def remove( key: String ) {
    map.remove( key )
    changed.set( true )
  }

  def isChanged: Boolean = changed.get()

  def flush() {
    try {
      val props = new Properties()
      map.foreach( (kv: (String,String)) => { props.put( kv._1, kv._2 ) } )
      val parentDir = new File( parentPath )
      if ( ! parentDir.exists() )
        parentDir.mkdirs()
      val file = new File( parentDir, path )
      if ( ! file.exists() )
        file.createNewFile()
      val fileStream = new FileOutputStream( file )
      props.storeToXML( fileStream, "bluemold store: " + name, "UTF-8" )
      fileStream.close()
    } catch {
      case t: Throwable => throw new RuntimeException( "Failed to store: " + name, t )
    }
  }

  def getFileContainer(name: String) = {
    val parentDir = new File( parentPath )
    val container = new File( parentDir, this.name + ".files/" + name )
    if ( ! container.exists() )
      container.mkdirs()
    container
  }
}