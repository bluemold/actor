package bluemold.io

import java.util.concurrent._
import java.io.{InputStream, IOException, OutputStream}


object Persist {
  val threadFactory = new ThreadFactory {
    def newThread( r: Runnable ) = {
      val thread = new Thread( r )
      thread.setPriority( Thread.NORM_PRIORITY )
      thread
    }
  }
  val maxWorkerThreads = 32
  val pool = new ThreadPoolExecutor(0, maxWorkerThreads, 60L, TimeUnit.SECONDS, new SynchronousQueue[Runnable], threadFactory)
  
  def write( data: Array[Byte], out: OutputStream, callback: (Boolean) => Unit ) {
    out.synchronized {
      data.synchronized {
        pool.execute( Write( data, out, callback ) )
      }
    }
  }
  def writeList( datas: List[Array[Byte]], out: OutputStream, callback: (Boolean) => Unit ) {
    out.synchronized {
      datas.synchronized {
        pool.execute( WriteList( datas, out, callback ) )
      }
    }
  }
  def read( data: Array[Byte], in: InputStream, callback: (Int) => Unit ) {
    in.synchronized {
      data.synchronized {
        pool.execute( Read( data, in, callback ) )
      }
    }
  }
}

case class Write( data: Array[Byte], out: OutputStream, callback: (Boolean) => Unit ) extends Runnable {
  def run() {
    try {
      var done = false
      out.synchronized {
        data.synchronized {
          try {
            out.write( data )
            out.flush()
            done = true
          } catch {
            case t: IOException => // done is false
          }
        }
      }
      callback( done )
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}

case class WriteList( datas: List[Array[Byte]], out: OutputStream, callback: (Boolean) => Unit ) extends Runnable {
  def run() {
    try {
      var done = false
      out.synchronized {
        datas.synchronized {
          try {
            datas foreach { out.write( _ ) }
            out.flush()
            done = true
          } catch {
            case t: IOException => // done is false
          }
        }
      }
      callback( done )
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}

case class Read( data: Array[Byte], in: InputStream, callback: (Int) => Unit ) extends Runnable {
  def run() {
    try {
      var read = -1
      in.synchronized {
        data.synchronized {
          try {
            read = in.read( data )
          } catch {
            case t: IOException => // read is -1
          }
        }
      }
      callback( read )
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}
