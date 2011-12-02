package bluemold.actor

import java.io.{OutputStream, InputStream}
import bluemold.io.{Persist => IOPersist}

object Persist {
  case class ReadBytes( read: Int, msg: Option[Any] )
  case class WroteBytes( success: Boolean, msg: Option[Any] )
  def read( data: Array[Byte], in: InputStream, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.read( data, in, ( read: Int ) => sender ! ReadBytes( read, replyWith ) )
  }
  def write( data: Array[Byte], out: OutputStream, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.write( data, out, ( success: Boolean ) => sender ! WroteBytes( success, replyWith ) )
  }
  def writeList( datas: List[Array[Byte]], out: OutputStream, replyWith: Option[Any] = None )(implicit sender: ActorRef) {
    IOPersist.writeList( datas, out, ( success: Boolean ) => sender ! WroteBytes( success, replyWith ) )
  }
}