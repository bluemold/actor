package bluemold.io

import java.io._
import java.nio.ByteBuffer
import bluemold.concurrent.{AtomicLong, AtomicReference}
import annotation.tailrec
import java.nio.charset.Charset
import java.nio.channels.FileChannel
import java.text.SimpleDateFormat
import java.util.Date

object Journal {
  val KB = 1024
  val defaultBlockSize = 64 * KB
  val defaultBlocksPerFile = 256
}
class Journal(dir: File, blockSize: Int, numBlocksPerFile: Int) {
  import Journal._
  def this(dirName: String, blockSize: Int, numBlocksPerFile: Int) = this(new File(dirName), blockSize, numBlocksPerFile)

  def this(dir: File) = this(dir, defaultBlockSize, defaultBlocksPerFile )

  def this(dirName: String) = this(new File(dirName))

  val innerBlockSize = blockSize - 16

  var entriesToWrite = new AtomicReference[List[LogEntry]](Nil)
  var pendingCount = new AtomicLong()

  val logFileNameFormat = new SimpleDateFormat( "yyyyMMdd-HHmmssSSS-" )

  def getPendingCount = pendingCount.get()

  val logger = new Thread( new Runnable {
    def run() {
      while ( true )
        try {
          val entries = poll()
          if (!entries.isEmpty) {
            println("Write entries: " + entries.length)
            writeEntries(entries)
          }
          synchronized { wait( 16 ) }
        } catch {
          case e: Exception => e.printStackTrace()
        }
    }
  } )
  logger.setDaemon( true )
  logger.start()

  @tailrec
  final def poll(): List[LogEntry] = {
    val old = entriesToWrite.get()
    if (!entriesToWrite.compareAndSet(old, Nil))
      poll()
    else old
  }

  var serializers: List[LogEntrySerializer] = Nil
  def addLogEntrySerializer( serializer: LogEntrySerializer ) {
    serializers ::= serializer
  }
  
  def forEntries( check: LogEntry => Boolean ) {
    var entry: LogEntry = null // TODO: get first
    while ( entry != null && check( entry ) ) {
       entry = null // TODO: get next
    }
  }
  
  def reverseForEntries( check: LogEntry => Boolean ) {
    var entry: LogEntry = null // TODO: get latest
    while ( entry != null && check( entry ) ) {
       entry = null // TODO: get prior
    }
  }

  final def log(msg: String) {
    log(MessageEntry(msg))
  }

  @tailrec
  final def log(logEntry: LogEntry) {
    val old = entriesToWrite.get()
    if (!entriesToWrite.compareAndSet(old, logEntry :: old))
      log(logEntry)
    else pendingCount.incrementAndGet()
  }

  def writeEntries(entries: List[LogEntry]) {
    val tail = getTailLogFile
    var (vBlock, entriesLeft) = createVBlock(tail.freeBlocks, entries.reverse)
    var vBlocks = vBlock :: Nil
    while (!entriesLeft.isEmpty) {
      val ret = createVBlock(numBlocksPerFile - 2, entriesLeft)
      vBlock = ret._1
      entriesLeft = ret._2
      vBlocks ::= vBlock
    }
    vBlocks.reverse foreach {
      writeVBlock _
    }
  }

  def writeVBlock(vBlock: VBlock) {
    val tail = getTailLogFile
    if (tail.hasEnoughBlocks(vBlock)) {
      tail.writeVBlock(vBlock)
      vBlock.signalEntries()
    } else {
      tail.writeEnd()
      val newTail = getNewLogFile
      if (newTail.hasEnoughBlocks(vBlock)) {
        newTail.writeVBlock(vBlock)
        vBlock.signalEntries()
      } else {
        throw new RuntimeException("What Happened!")
      }
    }
  }

  def createVBlock(freeBlocks: Int, entries: List[LogEntry]): (VBlock, List[LogEntry]) = {
    val maxVBlockSize = freeBlocks * innerBlockSize
    var currentSize = 16
    var entriesLeft = entries
    var vBlockEntries: List[LogEntry] = Nil
    var continueAdding = true
    while (continueAdding && currentSize < maxVBlockSize && !entriesLeft.isEmpty) {
      val entry = entriesLeft.head
      val entrySize = entry.size
      if (currentSize + 4 + entrySize <= maxVBlockSize) {
        currentSize += 4 + entrySize
        entriesLeft = entriesLeft.tail
        vBlockEntries ::= entry
      } else continueAdding = false
    }
    (new VBlock(this, vBlockEntries.reverse.toArray), entriesLeft)
  }

  var files: List[FileEntry] = Nil
  var nextFileSequenceNumber = 0L
  val directBuffer = ByteBuffer.allocateDirect(blockSize)

  def readBlock(f: File, index: Int) = {
    val bytes = new Array[Byte](blockSize)
    val fin = new RandomAccessFile(f, "r")
    try {
      fin.seek(index * blockSize)
      val read = fin.read(bytes, 0, blockSize)
      if (read != blockSize)
        throw new RuntimeException
    } finally {
      fin.close()
    }
    bytes
  }

  def writeBlock(f: File, index: Int, bytes: Array[Byte]) = {
    if (bytes.length < blockSize)
      throw new RuntimeException
    val raf = new RandomAccessFile(f, "rws")
    try {
      raf.seek(index * blockSize)
      raf.write(bytes, 0, blockSize)
    } finally {
      raf.close()
    }
    bytes
  }

  /**
   * 4 byte first signature
   * 4 byte block size
   * 4 byte blocks per file
   * 8 byte file sequence number
   * 4 byte state - preparing = 0, ready = 1, recycling = 2
   * ...
   * 4 byte wrote signature
   * 4 byte checksum
   */
  def isFirstWritten(bytes: Array[Byte]) = {
    bytes(0) == 'f' && bytes(1) == 'r' &&
      bytes(2) == 's' && bytes(3) == 't' &&
      bytes(blockSize - 8) == 'w' && bytes(blockSize - 7) == 'r' &&
      bytes(blockSize - 6) == 'o' && bytes(blockSize - 5) == 't'
  }

  /**
   * 4 byte write signature
   * 4 byte type - single = 0, start = 1, continue = 2, end = 3
   * 4 byte checksum
   * ...
   * 4 byte wrote signature
   */
  def isBlockWritten(bytes: Array[Byte]) = {
    bytes(0) == 'w' && bytes(1) == 'r' &&
      bytes(2) == 'i' && bytes(3) == 't' &&
      bytes(blockSize - 8) == 'w' && bytes(blockSize - 7) == 'r' &&
      bytes(blockSize - 6) == 'o' && bytes(blockSize - 5) == 't'
  }

  /**
   * 4 byte last signature
   * 4 byte vblocks written
   * 4 byte blocks written
   * 4 byte entries written
   * ...
   * 4 byte wrote signature
   */
  def isLastWritten(bytes: Array[Byte]) = {
    bytes(0) == 'l' && bytes(1) == 'a' &&
      bytes(2) == 's' && bytes(3) == 't' &&
      bytes(blockSize - 8) == 'w' && bytes(blockSize - 7) == 'r' &&
      bytes(blockSize - 6) == 'o' && bytes(blockSize - 5) == 't'
  }

  def readSequenceNumber(f: File) = {
    val bytes: Array[Byte] = readBlock(f, 0)
    if (isFirstWritten(bytes)) {
      var ret = 0L
      19.until(11, -1) foreach {
        i => ret = ret << 8 & bytes(i)
      }
      ret
    } else throw new RuntimeException
  }

  var logFiles: List[FileEntry] = {
    val files = dir.listFiles(new FilenameFilter {
      def accept(dir: File, name: String) = name.startsWith("log-") && name.endsWith(".dat")
    })
    if (files == null) Nil
    else (files map {
      f => FileEntry(f, readSequenceNumber(f))
    }).toList
  }


  def getTailDesc: FileEntry = {
    if (logFiles == null || logFiles.length == 0) null
    else if (logFiles.length == 1) logFiles(0)
    else logFiles.tail.foldLeft(logFiles.head)((b: FileEntry, e: FileEntry) => if (b.index > e.index) b else e)
  }

  nextFileSequenceNumber = getTailDesc match {
    case desc: FileEntry => desc.index + 1
    case null => 1L
  }

  if ( getTailDesc == null) {
    getNewLogFile
  }


  def getTailLogFile: DataFile = {
    val tailDesc = getTailDesc
    if (tailDesc == null) null
    else {
      val tailFile = new DataFile(this, tailDesc.file, blockSize, numBlocksPerFile, tailDesc.index)
      tailFile.findNextBlock()
      tailFile
    }
  }

  def getNewLogFile: DataFile = {
    if (!dir.exists() && !dir.mkdirs())
      throw new RuntimeException("Could not create log dir")
    new SimpleDateFormat( "yyyyMMdd-HHmmssSSS" )
    val temp = File.createTempFile( "log-"+logFileNameFormat.format( new Date() ) , ".dat", dir)
    val randomAccess = new RandomAccessFile(temp, "rws")
    try {
      randomAccess.setLength(blockSize * numBlocksPerFile)
    } finally {
      randomAccess.close()
    }
    val newSequenceNumber = nextFileSequenceNumber
    nextFileSequenceNumber += 1
    logFiles ::= FileEntry(temp, newSequenceNumber)
    val logFile = new DataFile(this, temp, blockSize, numBlocksPerFile, newSequenceNumber)
    logFile.writeFirst()
    logFile
  }

  case class FileEntry(file: File, index: Long)

  /**
   * 4 byte index in vblocks within file
   * 4 byte number of entries in vblock
   * 4 byte vblock length
   * 4 byte padding
   * ... ( entries )
   */
  class VBlock(log: Journal, entries: Array[LogEntry]) {
    private[io] val _log = log
    private[io] val _entries = entries
    val (blocks, blocksCount) = {
      var blocks: List[Block] = new Block(this, 0, 0) :: Nil
      var blocksCount = 1
      var index = 0
      val len = entries.length
      val last = len - 1
      0 until len foreach {
        i =>
          val entry = entries(i)
          val size = entry.size + 4
          if (index + size > log.innerBlockSize) {
            blocks ::= new Block(this, i, log.innerBlockSize - index)
            blocksCount += 1
            index += size - log.innerBlockSize
          } else if (index + size == log.innerBlockSize) {
            if (i != last) {
              blocks ::= new Block(this, i + 1, 0)
              blocksCount += 1
              index = 0
            }
          } else {
            index += size
          }
      }
      (blocks.reverse, blocksCount)
    }

    def numBlocks = blocksCount

    def getBlocks: List[Block] = blocks

    def signalEntries() {
      log.pendingCount.addAndGet(-entries.length)
      entries foreach {
        _.signalWritten()
      }
    }
  }

  class Block(vBlock: VBlock, startEntry: Int, startIndex: Int) {
    def writeTo(buf: ByteBuffer, blockType: Int) {
      val innerBlockSize = vBlock._log.innerBlockSize
      buf.put('w': Byte)
      buf.put('r': Byte)
      buf.put('i': Byte)
      buf.put('t': Byte)
      buf.putInt(blockType)
      var checksum = 0 // todo
      var written = 0
      var currentEntry = if (startIndex > 0) {
        if (startIndex < 4) {
          val entry = vBlock._entries(startEntry)
          val size = entry.size
          written += entry.writeSizeTo(buf, startIndex, 4 - startIndex)
          if (written + size > innerBlockSize)
            written += entry.writeTo(buf, 0, innerBlockSize - written)
          else
            written += entry.writeTo(buf, 0, size)
        } else {
          val entry = vBlock._entries(startEntry)
          val off = startIndex - 4
          val sizeLeft = entry.size - off
          if (written + sizeLeft > innerBlockSize)
            written += entry.writeTo(buf, off, innerBlockSize - written)
          else
            written += entry.writeTo(buf, off, sizeLeft)
        }
        startEntry + 1
      } else startEntry

      var entry = if (currentEntry < vBlock._entries.length) vBlock._entries(currentEntry) else null
      while (written < innerBlockSize && entry != null) {
        if (written + 4 > innerBlockSize)
          written += entry.writeSizeTo(buf, 0, innerBlockSize - written)
        else
          written += entry.writeSizeTo(buf, 0, 4)
        val size = entry.size
        if (written + size > innerBlockSize)
          written += entry.writeTo(buf, 0, innerBlockSize - written)
        else
          written += entry.writeTo(buf, 0, size)

        currentEntry += 1
        entry = if (currentEntry < vBlock._entries.length) vBlock._entries(currentEntry) else null
      }

      // null in the rest of the block if needed
      if (written < innerBlockSize) {
        var count = innerBlockSize - written
        while (count > 0) {
          buf.put(0: Byte)
          count -= 1
        }
      }

      buf.put('w': Byte)
      buf.put('r': Byte)
      buf.put('o': Byte)
      buf.put('t': Byte)
      buf.putInt(checksum)
    }
  }

  /**
   * frst last writ wrot
   */
  class DataFile(log: Journal, file: File, blockSize: Int, numBlocksPerFile: Int, sequenceNumber: Long) {
    var nextWriteBlock = 0
    var vBlocksWritten = 0
    var entriesWritten = 0

    def initialize() {
      // todo
    }

    def freeBlocks: Int = numBlocksPerFile - nextWriteBlock - 1

    def hasEnoughBlocks(vBlock: VBlock): Boolean = freeBlocks >= vBlock.numBlocks

    def findNextBlock() {
      var bytes = readBlock( file, 0 )
      if ( isFirstWritten( bytes ) ) {
        nextWriteBlock = 1
        bytes = readBlock( file, nextWriteBlock )
        while ( nextWriteBlock < numBlocksPerFile && isBlockWritten( bytes ) ) {
          nextWriteBlock+=1
          if ( nextWriteBlock < numBlocksPerFile ) {
            bytes = readBlock( file, nextWriteBlock )
          }
        }
      }
    }
    
    final def writeVBlock(vBlock: VBlock): Boolean = {
      val ret = writeBlocks(vBlock.getBlocks)
      vBlocksWritten += 1
      entriesWritten += vBlock._entries.length
      ret
    }

    /**
     * 4 byte write signature
     * 4 byte type - single = 0, start = 1, continue = 2, end = 3
     * ...
     * 4 byte wrote signature
     * 4 byte checksum
     */
    private def writeBlocks(blocks: List[Block]): Boolean = {
      val buf = log.directBuffer
      val raf = new RandomAccessFile(file, "rws")
      try {
        val channel = raf.getChannel
        channel.position(nextWriteBlock * blockSize)
        writeBlocks0(buf, channel, blocks, first = true)
      } catch {
        case e: Throwable => e.printStackTrace()
      } finally {
        raf.close()
      }
      true
    }

    @tailrec
    private def writeBlocks0(buf: ByteBuffer, channel: FileChannel, blocks: List[Block], first: Boolean): Boolean = {
      if (blocks.isEmpty) true
      else {
        val block = blocks.head
        val blockType = if (first) if (blocks.tail.isEmpty) 0 else 1 else if (blocks.tail.isEmpty) 3 else 2
        nextWriteBlock += 1
        buf.position(0) // start at the beginning
        block.writeTo(buf, blockType)
        buf.position(0) // start at the beginning
        channel.write(buf)
        writeBlocks0(buf, channel, blocks.tail, first = false)
      }
    }

    /**
     * 4 byte first signature
     * 4 byte block size
     * 4 byte blocks per file
     * 8 byte file sequence number
     * 4 byte state - preparing = 0, ready = 1, recycling = 2
     * ...
     * 4 byte wrote signature
     * 4 byte checksum
     */
    def writeFirst(): Boolean = {
      val buf = log.directBuffer
      val raf = new RandomAccessFile(file, "rws")
      try {
        val checksum = 0
        val channel = raf.getChannel
        channel.position(0)
        nextWriteBlock = 1
        buf.position(0)
        buf.put('f': Byte)
        buf.put('r': Byte)
        buf.put('s': Byte)
        buf.put('t': Byte)

        buf.putInt(blockSize)
        buf.putInt(numBlocksPerFile)
        buf.putLong(sequenceNumber)
        buf.putInt(1) // ready

        // fill the rest of the block
        val zeroOut = (blockSize - 32) / 8
        var i = 0
        while (i < zeroOut) {
          buf.putLong(0L)
          i += 1
        }

        // signature and checksum
        buf.put('w': Byte)
        buf.put('r': Byte)
        buf.put('o': Byte)
        buf.put('t': Byte)
        buf.putInt(checksum)
        buf.position(0)

        channel.write(buf)
      } finally {
        raf.close()
      }
      true
    }

    /**
     * 4 byte last signature
     * 4 byte vblocks written
     * 4 byte blocks written
     * 4 byte entries written
     * ...
     * 4 byte wrote signature
     * 4 byte checksum
     */
    def writeEnd(): Boolean = {
      val buf = log.directBuffer
      val raf = new RandomAccessFile(file, "rws")
      try {
        val checksum = 0
        val channel = raf.getChannel
        channel.position((numBlocksPerFile - 1) * blockSize)
        buf.position(0)
        buf.put('l': Byte)
        buf.put('a': Byte)
        buf.put('s': Byte)
        buf.put('t': Byte)

        buf.putInt(vBlocksWritten) // num vblocks
        buf.putInt(nextWriteBlock - 1) // num blocks
        buf.putInt(entriesWritten) // num entries

        // fill the rest of the block
        val zeroOut = (blockSize - 32) / 8
        var i = 0
        while (i < zeroOut) {
          buf.putLong(0L)
          i += 1
        }

        // signature and checksum
        buf.put('w': Byte)
        buf.put('r': Byte)
        buf.put('o': Byte)
        buf.put('t': Byte)
        buf.putInt(checksum)
        buf.position(0)

        nextWriteBlock = -1

        channel.write(buf)
      } finally {
        raf.close()
      }
      true
    }
  }

  /**
   * 4 byte length
   * ... ( bytes )
   */
  object LogEntry {
    @tailrec
    def readSize(buf: ByteBuffer, len: Int, priorBytes: Int): Int = {
      if (len > 0)
        readSize(buf, len - 1, priorBytes << 8 + (buf.get() & 0xff))
      else priorBytes
    }
  }

  trait LogEntrySerializer {
    def canParse( buf: ByteBuffer, off: Int, len: Int ): Boolean
    def parse( buf: ByteBuffer, off: Int, len: Int ): LogEntry
  }

  trait LogEntry {
    def size: Int

    def writeTo(buf: ByteBuffer, off: Int, len: Int): Int

    def writeSizeTo(buf: ByteBuffer, off: Int, len: Int): Int = {
      val value = size
      if (off == 0 && len > 0)
        buf.put(((value >> 24) & 0xff).toByte)
      if (off <= 1 && len + off > 1)
        buf.put(((value >> 16) & 0xff).toByte)
      if (off <= 2 && len + off > 2)
        buf.put(((value >> 8) & 0xff).toByte)
      if (off <= 3 && len + off > 3)
        buf.put((value & 0xff).toByte)

      if (len > 4 - off) 4 - off else len
    }

    def signalWritten() {}
  }

  object MessageEntry {
    val UTF8 = Charset.forName("UTF-8")
  }

  case class MessageEntry(msg: String) extends LogEntry {

    import MessageEntry._

    val bytes = msg.getBytes(UTF8)

    def size = bytes.length

    def writeTo(buf: ByteBuffer, off: Int, len: Int): Int = {
      val old = buf.position()
      buf.put(bytes, off, len)

      buf.position() - old
    }
  }

  object Test {
    def main(args: Array[String]) {
      val testLog = new Journal("testJournal")
      println("Writing...")
      val started = System.currentTimeMillis()
      var count = 0L
      while (System.currentTimeMillis() - started < 5000) {
        if (testLog.getPendingCount < 1000000)
          1 to 1000 foreach { _ =>
            testLog.log("HelloGoodbye")
            count+=1
          }
        else synchronized {
          wait(16)
        }
      }
      println("Waiting...")
      while (testLog.getPendingCount > 0) {
        synchronized {
          wait(16)
        }
      }
      val end = System.currentTimeMillis()
      println( "Bytes: " + ( count * 16 ) + " in " + ( end - started ) + "ms" )
    }
  }
}
