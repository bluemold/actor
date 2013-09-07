package bluemold.persist

import java.io.File
import bluemold.io._
import bluemold.concurrent.{AtomicReference, Future}
import annotation.tailrec
import java.nio.ByteBuffer
import bluemold.io.JournalEntryLocation
import collection.mutable
import java.util.concurrent.CountDownLatch

object RequestType extends Enumeration {
  type RequestType = Value
  val Undefined, Get, Put, PutIfEmpty, PutIfNotEmpty, PutIfExpected, Remove, RemoveIfExpected = Value
  def getTypeCode( rType: RequestType ) = rType match {
    case Get => 'G'
    case Put => 'P'
    case PutIfEmpty => 'I'
    case PutIfNotEmpty => 'U'
    case PutIfExpected => 'C'
    case Remove => 'R'
    case RemoveIfExpected => 'E'
    case Undefined => '?'
  }
  def getType( code: Char ) = code match {
    case 'G' => Get
    case 'P' => Put
    case 'I' => PutIfEmpty
    case 'U' => PutIfNotEmpty
    case 'C' => PutIfExpected
    case 'R' => Remove
    case 'E' => RemoveIfExpected
    case _ => Undefined
  }
}
import RequestType._

case class Request( rType: RequestType, rid: ByteBuffer, key: ByteBuffer, value: ByteBuffer, expected: ByteBuffer, future: Future[Result] )

class Result( val rType: RequestType, val rid: ByteBuffer, val priors: Int, val knowledge: Long,
              val key: ByteBuffer, val value: Option[ByteBuffer], val old: Option[ByteBuffer],
              val success: Boolean, val resolveTime: Long ) extends JournalEntry {
  def size = 2 + Serial.size(rid) +
    4 + 8 + Serial.size(key) +
    1 + ( if ( value eq None ) 0 else Serial.size( value.get ) ) +
    1 + ( if ( value eq None ) 0 else Serial.size( value.get ) ) + 1 + 8
  def writeTo(buf: ByteBuffer, off: Int, len: Int) = {
    val temp = ByteBuffer.allocate( size )
    Serial(temp)
      .putChar( RequestType.getTypeCode( rType ) )
      .putByteBuffer( rid )
      .putInt( priors )
      .putLong( knowledge )
      .putByteBuffer( key )
      .putBoolean( value ne None )
      .conditional( value ne None ).putByteBuffer( value.get )
      .putBoolean( old ne None )
      .conditional( old ne None ).putByteBuffer( old.get )
      .putBoolean( success )
      .putLong( resolveTime )
    temp.position(off)
    temp.limit(off+len)
    val oldPosition = buf.position()
    buf.put( temp )
    buf.position() - oldPosition
  }
}

class ResultSerializer extends JournalEntrySerializer {
  def canParse(buf: ByteBuffer, off: Int, len: Int) = RequestType.getType( Serial.getChar( buf, off ) ) ne Undefined 
  def parse(buf: ByteBuffer, off: Int, len: Int) = {
    val buffer = buf.duplicate()
    buffer.position( off )
    buffer.limit( off+len )
    val s = Serial(buffer)
    new Result( rType = RequestType.getType( s.getChar() ),
      rid = s.getByteBuffer(),
      priors = s.getInt(),
      knowledge = s.getLong(),
      key = s.getByteBuffer(),
      value = if ( s.getBoolean() ) Some(s.getByteBuffer()) else None,
      old = if ( s.getBoolean() ) Some(s.getByteBuffer()) else None,
      success = s.getBoolean(),
      resolveTime = s.getLong() )
  }
}

object TreeNode {
  val code = 'N'
  case class SerialJournalEntryLocation( s: Serial ) {
    def putJournalEntryLocation( loc: JournalEntryLocation ) = {
      if ( loc eq null ) s.putBoolean( value = false )
      else s.putBoolean( value = true )
        .putLong( loc.fileIndex )
        .putInt( loc.vBlock )
        .putInt( loc.entry )
      s
    } 
    def getJournalEntryLocation: JournalEntryLocation = {
      if ( ! s.getBoolean() ) null
      else JournalEntryLocation( s.getLong(), s.getInt(), s.getInt() )
    } 
  }
  implicit def _toSerialJournalEntryLocation( s: Serial ) = SerialJournalEntryLocation( s )
}

class TreeEntry( var key: ByteBuffer, var value: ByteBuffer, var afterThis: TreeNode, var afterThisLoc: JournalEntryLocation )
class TreeNode( var root: Boolean, var beforeThis: TreeNode, var beforeThisLoc: JournalEntryLocation, var entries: Array[TreeEntry] ) extends JournalEntry {
  import TreeNode._
  def size = 24 + entries.foldLeft(0) { (len: Int, e: TreeEntry) => len + e.key.remaining() + e.value.remaining() + 25 }
  def writeTo(buf: ByteBuffer, off: Int, len: Int) = {
    val temp = ByteBuffer.allocate( size )
    val s = Serial(temp)
      .putChar( code )
      .putBoolean( root )
      .putJournalEntryLocation( beforeThisLoc )
      .putInt( entries.length )
    entries foreach { e: TreeEntry => 
      s.putByteBuffer( e.key ).putByteBuffer( e.value ).putJournalEntryLocation( e.afterThisLoc )
    }
    temp.position(off)
    temp.limit(off+len)
    val oldPosition = buf.position()
    buf.put( temp )
    buf.position() - oldPosition
  }
  def isLeaf = beforeThisLoc eq null
}

class TreeNodeSerializer extends JournalEntrySerializer {
  import TreeNode._
  def canParse(buf: ByteBuffer, off: Int, len: Int) = Serial.getChar( buf, off ) == code 
  def parse(buf: ByteBuffer, off: Int, len: Int) = {
    val buffer = buf.duplicate()
    buffer.position( off )
    buffer.limit( off+len )
    val s = Serial(buffer)
    if ( s.getChar() != code ) throw new RuntimeException
    new TreeNode( root = s.getBoolean(), beforeThis = null, beforeThisLoc = s.getJournalEntryLocation, entries = getEntries(s) )
  }
  def getEntries( s: Serial ) = {
    val size = s.getInt()
    val entries = new Array[TreeEntry](size)
    0 until size foreach { i =>
      entries(i) = new TreeEntry( s.getByteBuffer(), s.getByteBuffer(), null, s.getJournalEntryLocation )
    }
    entries
  }
}



class JournaledIndex( incomingDir: File, indexDir: File ) {
  var requests = new AtomicReference[List[Request]](Nil)

  @tailrec private final def poll(): List[Request] = {
    val old = requests.get()
    if (!requests.compareAndSet(old, Nil))
      poll()
    else old.reverse
  }

  @tailrec private final def addRequest( request: Request) {
    val old = requests.get()
    if (!requests.compareAndSet(old, request :: old))
      addRequest(request)
  }

  def maxNodeEntries = 32

  var resolved: List[Result] = Nil
  
  val resolvedConfig = new ProcessJournalConfig() // Todo
  resolvedConfig add new ResultSerializer
  resolvedConfig add new JournalEntryProcessor {
    def isBatch = true
    override def process(entries: List[JournalEntry]) {
      // Todo: Create and submit changeSet to index
      // Todo: GC logic on resolved journal
    }
  }
  val resolvedJournal = new ProcessJournal( incomingDir, resolvedConfig ) {
    override def init() {
      // todo read in recent resolved requests
      walkEntriesReverse { (entry,loc) =>
        entry match {
          case result: Result =>
            resolved ::= result
          case _ =>
        }
        true
      }
    }
  }

  var rootLoc: JournalEntryLocation = null
  var root: TreeNode = null

  val indexConfig = new ProcessJournalConfig() // Todo
  indexConfig add new TreeNodeSerializer
  indexConfig add new JournalEntryProcessor {
    def isBatch = true

    override def process(entries: List[JournalEntry]) {
      // Todo: GC logic on index journal
    }
  }
  val indexJournal = new ProcessJournal( indexDir, indexConfig ) {
    override def init() {
      // todo read in top of current hash map
      walkEntriesReverse { (entry,loc) =>
        if ( entry.isInstanceOf[TreeNode] ) {
          val node = entry.asInstanceOf[TreeNode]
          if ( node.root ) {
            rootLoc = loc
            root = node
            false
          } else true
        } else true
      }
    }
  }

  def same( a: ByteBuffer, b: ByteBuffer ) = {
    if ( a.remaining() != b.remaining() ) false
    else {
      var same = true
      var ai = a.position()
      var bi = b.position()
      while ( same && ai < a.limit() ) {
        if ( a.get( ai ) != b.get( bi ) )
          same = false
        ai+=1
        bi+=1
      }
      same
    }
  }
  
  /* Returns true if a is less than b */
  def lessThan( a: ByteBuffer, b: ByteBuffer ) = {
    var same = true
    var less = false
    var ai = a.position()
    var bi = b.position()
    while ( same && ai < a.limit() && bi < b.limit() ) {
      if ( a.get( ai ) != b.get( bi ) ) {
        same = false
        less = a.get( ai ) < b.get( bi )
      }
      ai+=1
      bi+=1
    }
    if ( same && ai == a.limit() && bi < b.limit() )
      less = true
    less
  }

  @tailrec private def resolveRequests( set: TreeNodeChangeSet, requests: List[Request] ) {
    if ( ! requests.isEmpty ) {
      requests.head match {
        case Request( RequestType.Get, rid, key, _, _, future ) =>
          val prior = resolved find { res: Result => same( res.rid, rid ) }
          val knowledge = resolved.foldLeft(System.currentTimeMillis()) { (b:Long,res:Result) =>
            if ( b < res.resolveTime ) b
            else res.resolveTime 
          }
          val res = if ( prior.isEmpty ) {
            val value = get( set, key )
            new Result( RequestType.Get, rid, 0, knowledge, key, value, None, true, System.currentTimeMillis() )
          } else {
            val rOld = prior.get
            new Result( rOld.rType, rid, rOld.priors + 1, knowledge,
              rOld.key, rOld.value, rOld.old, rOld.success, System.currentTimeMillis() )
          }
          resolved ::= res
          resolvedJournal.log( new JournalEntryWithSignal( res, new JournalEntrySignal {
            override def signalWritten(loc: JournalEntryLocation, entry: JournalEntry) {
              set.latch.countDown()
              
            }
          }) )
        case Request( RequestType.Put, rid, key, value, _, future ) =>
        case Request( RequestType.PutIfEmpty, rid, key, value, _, future ) =>
        case Request( RequestType.PutIfNotEmpty, rid, key, value, _, future ) =>
        case Request( RequestType.PutIfExpected, rid, key, value, expected, future ) =>
        case Request( RequestType.Remove, rid, key, _, _, future ) =>
        case Request( RequestType.RemoveIfExpected, rid, key, _, expected, future ) =>
        case _ =>
      }
      resolveRequests( set, requests.tail )
    }
  }

  val incoming = new Thread( new Runnable {
    def run() {
      while ( true )
        try {
          val requests = poll()
          if (!requests.isEmpty) {
            val set = new TreeNodeChangeSet(requests.size)
            resolveRequests(set,requests.reverse)
            // Todo: setup trigger, once results persisted submit change set
          }
          synchronized { wait( 16 ) }
        } catch {
          case e: Exception => e.printStackTrace()
        }
    }
  } )
  incoming.setDaemon( true )
  incoming.start()

  val cache = new mutable.HashMap[JournalEntryLocation,TreeNode]
  
  class TreeNodeChangeSet( val numRequests: Int ) {
    var root: TreeNode = null
    var toRemove: List[JournalEntryLocation] = Nil
    val nodes = new mutable.HashSet[TreeNode]
    val parents = new mutable.HashMap[TreeNode,TreeNode]
    val cacheLocations = new mutable.HashMap[TreeNode,JournalEntryLocation]
    val cacheParents = new mutable.HashMap[JournalEntryLocation,JournalEntryLocation]
    val newParents = new mutable.HashMap[JournalEntryLocation,TreeNode]
    val latch = new CountDownLatch(numRequests)
    def contains( node: TreeNode ) = nodes.contains( node )
    def add( node: TreeNode, parent: TreeNode ) {
      if ( ! ( nodes contains parent ) ) throw new RuntimeException("WTF!")
      nodes add node
      parents.put(node,parent)
    }
    def getParent( node: TreeNode ) = parents get node
  }
  
  // each request will be resolved and logged even if it is a redundant request
  
  def get( rid: ByteBuffer, key: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.Get, rid, key, null, null, future ) )
    future
  }
  def put( rid: ByteBuffer, key: ByteBuffer, value: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.Put, rid, key, value, null, future ) )
    future
  }
  def putIfEmpty( rid: ByteBuffer, key: ByteBuffer, value: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.PutIfEmpty, rid, key, value, null, future ) )
    future
  }
  def putIfNotEmpty( rid: ByteBuffer, key: ByteBuffer, value: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.PutIfNotEmpty, rid, key, value, null, future ) )
    future
  }
  def putIfExpected( rid: ByteBuffer, key: ByteBuffer, value: ByteBuffer, expected: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.PutIfExpected, rid, key, value, expected, future ) )
    future
  }
  def remove( rid: ByteBuffer, key: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.Remove, rid, key, null, null, future ) )
    future
  }
  def removeIfExpected( rid: ByteBuffer, key: ByteBuffer, expected: ByteBuffer ): Future[Result] = {
    val future = new Future[Result]
    addRequest( Request( RequestType.RemoveIfExpected, rid, key, null, expected, future ) )
    future
  }

  private def resolve( set: TreeNodeChangeSet, parent: TreeNode, node: TreeNode, loc: JournalEntryLocation ): TreeNode = {
    if ( node ne null ) node
    else cache.get(loc) getOrElse {
      var found: TreeNode = null 
      indexJournal.entryRange( loc, 1, { ( entry, at ) => found = entry.asInstanceOf[TreeNode] } )
      if ( found ne null ) {
        cache.put(loc,found)
        // todo - record parentage
        
      }
      found
    }
  }
  
  private def get( set: TreeNodeChangeSet, key: ByteBuffer ): Option[ByteBuffer] = {
    val root0 = if ( set.root ne null ) set.root else root
    if ( root0 ne null )
      get0( set, key, root0 )
    else None
  }

  @tailrec private def get0( set: TreeNodeChangeSet, key: ByteBuffer, node: TreeNode ): Option[ByteBuffer] = {
    if ( node.isLeaf ) {
      node.entries find { entry => same( entry.key, key ) } map { _.value }
    } else {
      val found = node.entries.reverseIterator find { e => !lessThan( key, e.key ) }
      if ( found.isEmpty ) get0( set, key, resolve( set, node, node.beforeThis, node.beforeThisLoc ) )
      else {
        val e = found.get
        if ( same( key, e.key ) ) Some( e.value )
        else get0( set, key, resolve( set, node, e.afterThis, e.afterThisLoc )  )
      }
    }
  }

  private def insert( set: TreeNodeChangeSet, key: ByteBuffer, value: ByteBuffer ): Boolean = {
    val root0 = if ( set.root ne null ) set.root else root
    if ( root0 ne null )
      insert0( set, key, value, root0 )
    else false
  }

  @tailrec private def insert0( set: TreeNodeChangeSet, key: ByteBuffer, value: ByteBuffer, node: TreeNode ): Boolean = {
    if ( node.isLeaf ) {
      if ( node.entries.length < maxNodeEntries ) {
        val (before,after) = node.entries span { e => lessThan( e.key, key ) }
        val entries = Array.concat( before, Array( new TreeEntry( key, value, null, null ) ), after )
        if ( set.contains( node ) ) {
          node.entries = entries
          true
        }
        else {
          val leafClone = new TreeNode( node.root, null, null, entries )
          // todo - clone parents and add all to set
          true
        }
      } else {
        // todo split
        false
      }
    } else {
      val found = node.entries.reverseIterator find { e => !lessThan( key, e.key ) }
      if ( found.isEmpty ) insert0( set, key, value, resolve( set, node, node.beforeThis, node.beforeThisLoc ) )
      else {
        val e = found.get
        if ( same( key, e.key ) ) false
        else insert0( set, key, value, resolve( set, node, e.afterThis, e.afterThisLoc )  )
      }
    }
  }

  def remove( set: TreeNodeChangeSet, key: ByteBuffer, value: ByteBuffer ) {
    
  }
}