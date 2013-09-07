package bluemold.io

import java.io.File
import java.nio.ByteBuffer
import collection.immutable.HashMap
import bluemold.concurrent.AtomicReference
import annotation.tailrec

trait WrappedJournalEntry extends JournalEntry {
  def unwrap: JournalEntry
}

class ChannelJournalEntry( channel: String, entry: JournalEntry ) extends WrappedJournalEntry {
  def getChannel = channel
  def getEntry = entry
  def size = 6 + Serial.encodeLength( channel ) + entry.size

  def writeTo(buf: ByteBuffer, off: Int, len: Int) = {
    val channelNameLength = Serial.encodeLength( channel )
    val bytes = new Array[Byte](6+channelNameLength)
    Serial.putChar( bytes, 0, 'C' )
    Serial.putInt( bytes, 2, channelNameLength )
    Serial.encode( bytes, channel )

    val headerLength = bytes.length
    val old = buf.position()
    
    if ( off + len <= headerLength )
      buf.put(bytes, off, len)
    else if ( off < headerLength ) {
      val headerWritten = headerLength - off
      buf.put(bytes, off, headerWritten)
      entry.writeTo( buf, 0, len - headerWritten )
    }
    else entry.writeTo( buf, off - headerLength, len )

    buf.position() - old
  }

  def unwrap = entry
}

class ChannelJournalEntrySerializer extends JournalEntrySerializer {
  def canParse(buf: ByteBuffer, off: Int, len: Int) =
    if ( len > 6 && Serial.getChar(buf,off) == 'C' ) {
      val channelNameLength = Serial.getInt(buf,off+2)
      val headerLength = 6 + channelNameLength
      if ( len > headerLength ) {
        val channel = Serial.decode(buf,off+6,channelNameLength)
        ( channelSerializers.get(channel).getOrElse(Nil)
          .find(_.canParse(buf,off+headerLength,len-headerLength)) ne None ) ||
        ( serializers
          .find(_.canParse(buf,off+headerLength,len-headerLength)) ne None )
      } else false
    } else false 
  def parse(buf: ByteBuffer, off: Int, len: Int) =
    if ( len > 6 && Serial.getChar(buf,off) == 'C' ) {
      val channelNameLength = Serial.getInt(buf,off+2)
      val headerLength = 6 + channelNameLength
      if ( len > headerLength ) {
        val channel = Serial.decode(buf,off+6,channelNameLength)
        channelSerializers.get(channel).getOrElse(Nil)
          .find(_.canParse(buf,off+headerLength,len-headerLength)) match {
          case Some(serializer:JournalEntrySerializer) => new ChannelJournalEntry( channel, serializer.parse(buf,off+headerLength,len-headerLength) )
          case None => serializers.find(_.canParse(buf,off+headerLength,len-headerLength)) match {
            case Some(serializer:JournalEntrySerializer) => new ChannelJournalEntry( channel, serializer.parse(buf,off+headerLength,len-headerLength) )
            case None => null
          }
        }
      } else null
    } else null 

  var serializers: List[JournalEntrySerializer] = Nil
  def add( serializer: JournalEntrySerializer ): ChannelJournalEntrySerializer = {
    serializers ::= serializer
    this
  }
  var channelSerializers = new HashMap[String, List[JournalEntrySerializer]]
  def add( channel: String, serializer: JournalEntrySerializer ): ChannelJournalEntrySerializer = {
    channelSerializers.get(channel) match {
      case Some(list:List[JournalEntrySerializer]) => channelSerializers += ((channel,serializer::list))
      case None => channelSerializers += ((channel,serializer::Nil))
    }
    this
  }
}

class ChannelProcessJournalEntry( channel: String, fileIndex: Long, vBlock: Int, entry: Int ) extends JournalEntry {
  def size = 22 + Serial.encodeLength( channel )
  def writeTo(buf: ByteBuffer, off: Int, len: Int) = {
    val bytes = new Array[Byte](size)
    val channelNameLength = Serial.encodeLength( channel )
    Serial.putChar(bytes,0,'p')
    Serial.putInt(bytes,2,channelNameLength)
    Serial.encode(bytes,channel)
    Serial.putLong(bytes,6+channelNameLength,fileIndex)
    Serial.putInt(bytes,14+channelNameLength,vBlock)
    Serial.putInt(bytes,18+channelNameLength,entry)

    val old = buf.position()
    buf.put(bytes, off, len)
    buf.position() - old
  }
}

class ProcessJournalEntry( fileIndex: Long, vBlockIndex: Int, entryIndex: Int ) extends JournalEntry {
  def getFileIndex = fileIndex
  def getVBlockIndex = vBlockIndex
  def getEntryIndex = entryIndex
  def size = 18
  def writeTo(buf: ByteBuffer, off: Int, len: Int) = {
    val bytes = new Array[Byte](size)
    Serial.putChar(bytes,0,'P')
    Serial.putLong(bytes,2,fileIndex)
    Serial.putInt(bytes,10,vBlockIndex)
    Serial.putInt(bytes,14,entryIndex)

    val old = buf.position()
    buf.put(bytes, off, len)
    buf.position() - old
  }
}

class ChannelProcessJournalEntrySerializer extends JournalEntrySerializer {
  def canParse(buf: ByteBuffer, off: Int, len: Int) =
    if ( len > 6 && Serial.getChar(buf,off) == 'p' ) {
      val channelNameLength = Serial.getInt(buf,off+2)
      val headerLength = 6 + channelNameLength
      len == headerLength + 16
    } else false 
  def parse(buf: ByteBuffer, off: Int, len: Int) =
    if ( len > 6 && Serial.getChar(buf,off) == 'p' ) {
      val channelNameLength = Serial.getInt(buf,off+2)
      val headerLength = 6 + channelNameLength
      if ( len == headerLength + 16 ) {
        val channel = Serial.decode(buf,off+6,channelNameLength)
        val fileIndex = Serial.getLong(buf,off+headerLength)
        val vBlock = Serial.getInt(buf,off+headerLength+8)
        val entry = Serial.getInt(buf,off+headerLength+12)
        new ChannelProcessJournalEntry( channel, fileIndex, vBlock, entry )
      } else null
    } else null
}

class ProcessJournalEntrySerializer extends JournalEntrySerializer {
  def canParse(buf: ByteBuffer, off: Int, len: Int) =
    len == 18 && Serial.getChar(buf,off) == 'P'
  def parse(buf: ByteBuffer, off: Int, len: Int) =
    if ( len == 18 && Serial.getChar(buf,off) == 'P' ) {
      val fileIndex = Serial.getLong(buf,off+2)
      val vBlock = Serial.getInt(buf,off+10)
      val entry = Serial.getInt(buf,off+14)
      new ProcessJournalEntry( fileIndex, vBlock, entry )
    } else null
}

trait JournalEntryProcessor {
  def isBatch: Boolean
  def process( entry: JournalEntry ) {}
  def process( entries: List[JournalEntry] ) {}
}

class ProcessJournalConfig extends JournalConfig {
  val channelSerializer = new ChannelJournalEntrySerializer
  super.add( channelSerializer )
  super.add( new ProcessJournalEntrySerializer )

  override def add(serializer: JournalEntrySerializer) = { channelSerializer.add( serializer ); this }
  def add( channel: String, serializer: JournalEntrySerializer) = { channelSerializer.add( channel, serializer ); this }

  var processors: List[JournalEntryProcessor] = Nil
  def add( processor: JournalEntryProcessor ) = {
    processors ::= processor
    this
  }
  var channelProcessors = new HashMap[String, List[JournalEntryProcessor]]
  def add( channel: String, processor: JournalEntryProcessor ) = {
    channelProcessors.get(channel) match {
      case Some(list:List[JournalEntryProcessor]) => channelProcessors += ((channel,processor::list))
      case None => channelProcessors += ((channel,processor::Nil))
    }
    this
  }
}

object ProcessJournal {
  case object DefaultConfig extends ProcessJournalConfig
}

class ProcessJournal( dir: File, config: ProcessJournalConfig ) extends Journal( dir, config ) {
  def this( dir: File ) = this( dir, ProcessJournal.DefaultConfig )
  def this(dirName: String) = this(new File(dirName))
  def this(dirName: String, config: ProcessJournalConfig ) = this(new File(dirName), config )

  var setsToProcess = new AtomicReference[List[SetToProcess]](Nil)

  findLastProcessed()

  private def findLastProcessed() {
    var continue = true
    var file = getTailLogFile
    var lastProcessed: JournalEntryLocation = null
    while ( continue && file != null ) {
      var vBlocks: List[VBlock] = Nil 
      var vBlock = file.readVBlock
      while ( vBlock != null ) {
        vBlocks ::= vBlock
        vBlock = file.readVBlock
      }
      while ( continue && ! vBlocks.isEmpty ) {
        vBlock = vBlocks.head
        vBlocks = vBlocks.tail
        val entries = vBlock._entries
        val len = entries.length
        if ( lastProcessed == null ) {
          var i = len - 1
          while ( lastProcessed == null && i >= 0 ) {
            val entry = entries(i)
            entry match {
              case hEntry: ProcessJournalEntry =>
                lastProcessed = JournalEntryLocation(hEntry.getFileIndex, hEntry.getVBlockIndex, hEntry.getEntryIndex)
              case _ => i -= 1
            }
          }
        }
        if ( lastProcessed != null && vBlock._fileIndex == lastProcessed.fileIndex && vBlock._vBlockIndex == lastProcessed.vBlock ) {
          if ( len > lastProcessed.entry + 1 ) {
            addToProcess( SetToProcess( entries, JournalEntryLocation( lastProcessed.fileIndex, lastProcessed.vBlock, lastProcessed.entry + 1 ) ) )
          }
          continue = false
        } else {
          addToProcess( SetToProcess( entries, JournalEntryLocation( lastProcessed.fileIndex, lastProcessed.vBlock, 0 ) ) )
        }
      }
      if ( continue )
        file = getPriorLogFile( file )
    }
  }

  val processor = new Thread( new Runnable {
    def run() {
      while ( true )
        try {
          val sets = pollToProcess()
          if (!sets.isEmpty) {
            println("Process sets: " + sets.length)
            processSets(sets.reverse)
          }
          synchronized { wait( 16 ) }
        } catch {
          case e: Exception => e.printStackTrace()
        }
    }
  } )
  processor.setDaemon( true )
  processor.start()

  def processSets( sets: List[SetToProcess] ) { processSets0( sets ) }

  @tailrec
  final def processSets0( sets: List[SetToProcess] ) {
    if ( ! sets.isEmpty ) {
      val set = sets.head
      val loc = set.startingLocation
      var i = loc.entry
      var backwards: List[JournalEntry] = Nil
      var entryMap: HashMap[String,List[JournalEntry]] = HashMap.empty
      while ( i < set.entries.length ) {
        val sEntry = set.entries(i)
        sEntry match {
          case cEntry: ChannelJournalEntry =>
            val channel = cEntry.getChannel
            val entry = cEntry.getEntry
            val bucket = entryMap(channel)
            if (bucket eq null) entryMap += ((channel, entry :: Nil))
            else entryMap += ((channel, entry :: bucket))
            backwards ::= entry
          case _ =>
        }
        i+=1
      }
      val entries = backwards.reverse
      val (batchers,singlers) = config.processors partition { _.isBatch }
      batchers foreach { _.process( entries ) }
      entries foreach { entry => singlers foreach { _.process( entry ) } }
      entryMap foreach { pair =>
        val (channel,backwards) = pair
        val entries = backwards.reverse
        config.channelProcessors.get( channel ) match {
          case Some( processors ) =>
            val (batchers,singlers) = processors partition { _.isBatch }
            batchers foreach { _.process( entries ) }
            entries foreach  { entry => singlers foreach { _.process( entry ) } }
          case None =>
        }
      }
      log( new ProcessJournalEntry( loc.fileIndex, loc.vBlock, set.entries.length - 1 ) )
      processSets0( sets.tail )
    }
  }

  @tailrec
  final def pollToProcess(): List[SetToProcess] = {
    val old = setsToProcess.get()
    if (!setsToProcess.compareAndSet(old, Nil))
      pollToProcess()
    else old
  }

  def addToProcess(set: SetToProcess) { addToProcess0(set) }

  @tailrec
  final def addToProcess0(set: SetToProcess) {
    val old = setsToProcess.get()
    if (!setsToProcess.compareAndSet(old, set :: old))
      addToProcess0(set)
  }

  case class SetToProcess( entries: Array[JournalEntry], startingLocation: JournalEntryLocation )

  override def writeVBlock( vBlock: VBlock) {
    val tail = getTailLogFile
    if ( tail.hasEnoughBlocks(vBlock) ) {
      if ( tail.fileEntry.index == vBlock._fileIndex &&
        tail.vBlocksWritten == vBlock._vBlockIndex ) {
        println( classOf[ProcessJournal].getName + ": WTF! VBlock location does not match current log file!")
      }
      tail.writeVBlock(vBlock)
      vBlock.signalEntries(tail.fileEntry.index,tail.vBlocksWritten-1)
      addToProcess( SetToProcess( vBlock._entries, JournalEntryLocation( vBlock._fileIndex, vBlock._vBlockIndex, 0 ) ) )
    } else {
      tail.writeEnd()
      val newTail = getNewLogFile
      if ( newTail.hasEnoughBlocks(vBlock) ) {
        if ( newTail.fileEntry.index == vBlock._fileIndex &&
          newTail.vBlocksWritten == vBlock._vBlockIndex ) {
          println( classOf[ProcessJournal].getName + ": WTF! VBlock location does not match current log file! - spot number 2")
        }
        newTail.writeVBlock(vBlock)
        vBlock.signalEntries(newTail.fileEntry.index,newTail.vBlocksWritten-1)
        addToProcess( SetToProcess( vBlock._entries, JournalEntryLocation( vBlock._fileIndex, vBlock._vBlockIndex, 0 ) ) )
      } else {
        throw new RuntimeException("What Happened!")
      }
    }
  }
}

