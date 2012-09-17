package bluemold.io

import java.io.File

trait JournalEntryProcessor {
  def process( entry: JournalEntry ): Boolean
}

trait ProcessJournalConfig extends JournalConfig {
  var processors: List[JournalEntryProcessor] = Nil
  def addLogEntryProcessor( processor: JournalEntryProcessor ) {
    processors ::= processor
  }
}

object ProcessJournal {
  case object DefaultConfig extends ProcessJournalConfig {
    def blockSize = Journal.DefaultConfig.blockSize
    def numBlocksPerFile = Journal.DefaultConfig.blockSize
  }
}

class ProcessJournal( dir: File, config: JournalConfig ) extends Journal( dir, config ) {
  import ProcessJournal._
  def this( dir: File ) = this( dir, DefaultConfig )
  def this(dirName: String) = this(new File(dirName))
  def this(dirName: String, config: JournalConfig ) = this(new File(dirName), config )


}

