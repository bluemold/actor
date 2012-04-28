package bluemold.actor

import java.io.{File, OutputStream, InputStream}
import bluemold.io.{AsyncReader, AsyncWriter, Persist => IOPersist}

object Persist {
  case class ReadBytes( read: Int, msg: Option[Any] )
  case class WroteBytes( success: Boolean, msg: Option[Any] )

  def getAsyncReader( file: File ) = IOPersist.getAsyncReader( file ) 
  def getAsyncWriter( file: File ) = IOPersist.getAsyncWriter( file ) 
  
  def read( data: Array[Byte], in: InputStream, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.read( data, in, ( read: Int ) => sender ! ReadBytes( read, replyWith ) )
  }
  def write( data: Array[Byte], out: OutputStream, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.write( data, out, ( success: Boolean ) => sender ! WroteBytes( success, replyWith ) )
  }
  def writeList( datas: List[Array[Byte]], out: OutputStream, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.writeList( datas, out, ( success: Boolean ) => sender ! WroteBytes( success, replyWith ) )
  }
  def writeAsync( data: Array[Byte], offset: Long, out: AsyncWriter, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.writeAsync( data, offset, out, ( success: Boolean ) => sender ! WroteBytes( success, replyWith ) )
  }
  def readAsync( data: Array[Byte], offset: Long, in: AsyncReader, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.readAsync( data, offset, in, ( read: Int ) => sender ! ReadBytes( read, replyWith ) )
  }
}