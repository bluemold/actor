package bluemold.io

import java.util.concurrent._
import java.io._
import java.nio.ByteBuffer


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
  
  def getAsyncReader( file: File ) = new AsyncReader( file ) 
  def getAsyncWriter( file: File ) = new AsyncWriter( file ) 
  
  def write( data: Array[Byte], out: OutputStream, callback: (Boolean) => Unit ) {
    out.synchronized {
      data.synchronized {
        try {
          pool.execute( Write( data, out, callback ) )
        } catch {
          case e: RejectedExecutionException => callback( false )
        }
      }
    }
  }
  def writeList( datas: List[Array[Byte]], out: OutputStream, callback: (Boolean) => Unit ) {
    out.synchronized {
      datas.synchronized {
        try {
          pool.execute( WriteList( datas, out, callback ) )
        } catch {
          case e: RejectedExecutionException => callback( false )
        }
      }
    }
  }
  def read( data: Array[Byte], in: InputStream, callback: (Int) => Unit ) {
    in.synchronized {
      data.synchronized {
        try {
          pool.execute( Read( data, in, callback ) )
        } catch {
          case e: RejectedExecutionException => callback( 0 )
        }
      }
    }
  }

  def writeAsync( data: Array[Byte], offset: Long, out: AsyncWriter, callback: (Boolean) => Unit ) {
    data.synchronized {
      try {
        pool.execute( WriteToWriter( data, offset, out, callback ) )
      } catch {
        case e: RejectedExecutionException => callback( false )
      }
    }
  }
  def readAsync( data: Array[Byte], offset: Long, in: AsyncReader, callback: (Int) => Unit ) {
    data.synchronized {
      try {
        pool.execute( ReadFromReader( data, offset, in, callback ) )
      } catch {
        case e: RejectedExecutionException => callback( 0 )
      }
    }
  }
}

class AsyncReader( val file: File ) {
  private[io] val randomAccess = new RandomAccessFile( file, "r" )
  private[io] val channel = randomAccess.getChannel
  private[io] def read( bytes: Array[Byte], offset: Long ): Int = {
    val buffer = ByteBuffer.wrap( bytes )
    val read = channel.read( buffer, offset )
    if ( read == 0 ) {
      synchronized { wait( 15 ) }
      val read = channel.read( buffer, offset )
      if ( read == 0 ) {
        synchronized { wait( 15 ) }
        channel.read( buffer, offset )
      } else read
    } else read
  }
  def close() { randomAccess.close() }
}

class AsyncWriter( val file: File ) {
  private[io] val randomAccess = new RandomAccessFile( file, "rws" )
  private[io] val channel = randomAccess.getChannel
  private[io] def read( bytes: Array[Byte], offset: Long ): Int = {
    val buffer = ByteBuffer.wrap( bytes )
    val read = channel.read( buffer, offset )
    if ( read == 0 ) {
      synchronized { wait( 15 ) }
      val read = channel.read( buffer, offset )
      if ( read == 0 ) {
        synchronized { wait( 15 ) }
        channel.read( buffer, offset )
      } else read
    } else read
  }
  private[io] def write( bytes: Array[Byte], offset: Long ): Int = {
    val buffer = ByteBuffer.wrap( bytes )
    val written = channel.write( buffer, offset )
    if ( written == 0 ) {
      synchronized { wait( 15 ) }
      val written = channel.write( buffer, offset )
      if ( written == 0 ) {
        synchronized { wait( 15 ) }
        val written = channel.write( buffer, offset )
        if ( written != 0 ) channel.force( true )
        written
      } else {
        channel.force( true )
        written
      }
    } else {
      channel.force( true )
      written
    }
  }
  def close() { randomAccess.close() }
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
            case t: IOException => t.printStackTrace() // done is false
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
        datas foreach {
          data => data.synchronized {
            try {
              out.write( data )
              out.flush()
              done = true
            } catch {
              case t: IOException => t.printStackTrace() // done is false
            }
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
            case t: IOException => t.printStackTrace() // read is -1
          }
        }
      }
      callback( read )
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}

case class WriteToWriter( data: Array[Byte], offset: Long, out: AsyncWriter, callback: (Boolean) => Unit ) extends Runnable {
  def run() {
    try {
      var done = false
      data.synchronized {
        try {
          done = out.write( data, offset ) == data.length
        } catch {
          case t: IOException => t.printStackTrace() // done is false
        }
      }
      callback( done )
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}

case class ReadFromReader( data: Array[Byte], offset: Long, in: AsyncReader, callback: (Int) => Unit ) extends Runnable {
  def run() {
    try {
      var read = -1
      data.synchronized {
        try {
          read = in.read( data, offset )
        } catch {
          case t: IOException => t.printStackTrace() // read is -1
        }
      }
      callback( read )
    } catch {
      case t: Throwable => t.printStackTrace()
    }
  }
}
