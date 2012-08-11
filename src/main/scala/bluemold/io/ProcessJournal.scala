package bluemold.io

import java.io.File

class ProcessJournal( dir: File, blockSize: Int, numBlocksPerFile: Int ) extends Journal( dir, blockSize, numBlocksPerFile ) {
  import Journal._
  def this( dir: File ) = this( dir, defaultBlockSize, defaultBlocksPerFile )

  trait LogEntryProcessor {
    def process( entry: LogEntry ): Boolean
  }

  var processors: List[LogEntryProcessor] = Nil
  def addLogEntryProcessor( processor: LogEntryProcessor ) {
    processors ::= processor
  }
}

