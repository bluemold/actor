package bluemold.actor

import scala.PartialFunction
import collection.immutable.HashMap
import scala.util.Random

object GroupActor {
  sealed class GroupActorState
  case object Initializing extends GroupActorState
  case object Singleton extends GroupActorState
  case object ElectionPrep extends GroupActorState
  case object Election extends GroupActorState
  case object Normal extends GroupActorState
  case object Waiting extends GroupActorState
  
  sealed class GroupActorUpdateState
  case object Stable extends GroupActorUpdateState
  case object PushingUpdate extends GroupActorUpdateState
  case object ReceivingUpdate extends GroupActorUpdateState

  sealed class GroupActorMessage
  case class AnyoneThere( me: ActorRef, state: GroupActorState, leader: ActorRef, members: List[ActorRef] ) extends GroupActorMessage
  case class ImHere( me: ActorRef, state: GroupActorState, leader: ActorRef, members: List[ActorRef] ) extends GroupActorMessage

  case class JoinRequest( me: ActorRef ) extends GroupActorMessage
  case class UpdateRequest( me: ActorRef, groupState: HashMap[String,Any] ) extends GroupActorMessage

  case class BeginUpdate( leader: ActorRef, seq: Long, members: List[ActorRef], groupState: HashMap[String,Any] ) extends GroupActorMessage
  case class AcknowledgeUpdate( me: ActorRef, leader: ActorRef, seq: Long, accepted: Boolean ) extends GroupActorMessage
  case class CommitUpdate( leader: ActorRef, seq: Long, members: List[ActorRef], groupState: HashMap[String,Any] ) extends GroupActorMessage
  case class CancelUpdate( leader: ActorRef, seq: Long, members: List[ActorRef] ) extends GroupActorMessage

  case class Halt( leader: ActorRef, seq: Long, members: List[ActorRef], groupState: HashMap[String,Any] ) extends GroupActorMessage 
  case class AcknowledgeHalt( me: ActorRef, leader: ActorRef, seq: Long, accepted: Boolean ) extends GroupActorMessage
  case class ImLeader( leader: ActorRef, seq: Long, members: List[ActorRef], groupState: HashMap[String,Any] ) extends GroupActorMessage

  sealed abstract class GroupDefinition {
    val random = new Random()
    def requestPotentialMembers( actor: GroupActor )( implicit self: ActorRef )
    def getTimeout: Long = 300 + random.nextInt( 50 ); // in milliseconds
    def getHeartbeatTimeout: Long = 3000 + random.nextInt( 50 ); // in milliseconds
    def quorumNumerator: Int = 2
    def quorumDenominator: Int = 3
  }
  final class AllGroup( name: String ) extends GroupDefinition {
    def requestPotentialMembers( actor: GroupActor )( implicit self: ActorRef ) {
      val cluster = self.getCurrentStrategy().getCluster
      println( "Requesting anyone out there" )
      cluster.sendAllWithId( name, AnyoneThere( self, actor.state, actor.leader, actor.members ) )
    }
  }
  final class InterfaceGroup( name: String, interface: NodeInterface ) extends GroupDefinition {
    def requestPotentialMembers( actor: GroupActor )( implicit self: ActorRef ) {
      val cluster = self.getCurrentStrategy().getCluster
      cluster.sendAllWithId( name, AnyoneThere( self, actor.state, actor.leader, actor.members ) )
    }
  }
  final class ExpandableGroup( name: String, initial: List[NodeRoute] )  extends GroupDefinition {
    var potentialNodes = initial
    def requestPotentialMembers( actor: GroupActor )( implicit self: ActorRef ) {
      val cluster = self.getCurrentStrategy().getCluster
      potentialNodes foreach { route => cluster.sendAllWithId( route.target, name, AnyoneThere( self, actor.state, actor.leader, actor.members ) ) }
    }
  }
  final class PredefinedGroup( name: String, nodes: List[NodeRoute] )  extends GroupDefinition {
    def requestPotentialMembers( actor: GroupActor )( implicit self: ActorRef ) {
      val cluster = self.getCurrentStrategy().getCluster
      nodes foreach { route => cluster.sendAllWithId( route.target, name, AnyoneThere( self, actor.state, actor.leader, actor.members ) ) }
    }
  }
}

trait GroupActor extends RegisteredActor {
  import GroupActor._

  var state: GroupActorState = _
  var potentialMembers: List[ActorRef] = _
  var members: List[ActorRef] = _
  var leader: ActorRef = _

  var electionSeq: Long = _
  var updateSeq: Long = _

  var sharedState: HashMap[String,Any] = _
  var updateState: GroupActorUpdateState = _
  var newGroupState: HashMap[String,Any] = _
  var newGroupStateTime: Long = _

  // timeout control
  var waitingTill: Long = _
  var timeoutEvent: CancelableEvent = _
  var waitingFor: List[ActorRef] = _

  // transitory
  var repliedPriority: ActorRef = _
  var memberReplies: List[ActorRef] = _
  var haltLeader: ActorRef = _
  var haltSeq: Long = _
  var receivedUpdateSeq: Long = _

  def definition: GroupDefinition

  protected def initGroupActor() {
    state = Initializing
    potentialMembers = Nil
    members = Nil
    memberReplies = Nil
    leader = self
    updateState = Stable
    sharedState = new HashMap[String,Any]
    newGroupState = new HashMap[String,Any]
    newGroupStateTime = 0
    val timeout = definition.getTimeout
    definition.requestPotentialMembers( this )
    waitingTill = System.currentTimeMillis() + timeout
    timeoutEvent = onTimeout( timeout ) { startFirstElection() }
  }

  protected def requestGroupUpdate( requestedGroupState: HashMap[String,Any] ) {
    if ( state == Normal && updateState == Stable ) {
      if ( isLeader ) {
        beginGroupUpdate( requestedGroupState )
      } else {
        leader ! UpdateRequest( self, requestedGroupState )
      }
    }
  }

  private def updateState( newState: GroupActorState ) {
    state = newState
    stateChanged( newState )
  }

  protected def stateChanged( state: GroupActorState )
  protected def sharedStateChanged( groupState: HashMap[String, Any] )
  protected def leaderChanged( iamleader: Boolean )

  protected def currentSharedState = sharedState
  protected def currentState = state
  protected def isLeader = leader != null && leader._getUUID == getUUID

  private def hasPriorityOverMe( member: ActorRef ): Boolean = hasPriorityOverThem( member, self )

  private def hasPriorityOverThem( member: ActorRef, them: ActorRef ): Boolean = {
    val theirUuid = them._getUUID
    val memberUuid = member._getUUID
    val ret = theirUuid.rand compareTo memberUuid.rand
    if ( ret == 0 ) {
      val ret = theirUuid.time compareTo memberUuid.time
      if ( ret == 0 ) {
        val ret = theirUuid.clusterId.rand compareTo memberUuid.clusterId.rand
        if ( ret == 0 ) {
          val ret = theirUuid.clusterId.time compareTo memberUuid.clusterId.time
          if ( ret == 0 ) {
            throw new RuntimeException( "Can not ask if a group actor has priority over itself" )
          } else ret > 0
        } else ret > 0
      } else ret > 0
    } else ret > 0
  }

  private def startFirstElection() {
    if ( state == Initializing ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { startFirstElection() }
      } else {
        startElection()
      }
    }
  }

  private def startElection() {
    val timeout = definition.getTimeout
    updateState( ElectionPrep )
    potentialMembers filter { _ != self } foreach { member => if ( hasPriorityOverMe( member ) ) member ! AnyoneThere( self, state, leader, members ) }
    waitingTill = System.currentTimeMillis() + timeout
    timeoutEvent = onTimeout( timeout ) { noPriorityRepliesForElection() }
  }

  private def aPriorityRepliedForElection( replied: ActorRef ) {
    if ( state == ElectionPrep && ( repliedPriority == null || ( replied != repliedPriority && hasPriorityOverThem( replied, repliedPriority ) ) ) ) {
      repliedPriority = replied
      val timeout = definition.getTimeout
      updateState( Waiting )
      waitingTill = System.currentTimeMillis() + timeout
      if ( timeoutEvent != null ) timeoutEvent.cancel()
      timeoutEvent = onTimeout( timeout ) { requestJoinAfterWaitingForRepliedPriority() }
    }
  }

  private def requestJoinAfterWaitingForRepliedPriority() {
    if ( state == Waiting ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { requestJoinAfterWaitingForRepliedPriority() }
      } else { 
        val timeout = definition.getTimeout
        repliedPriority ! JoinRequest( self )
        waitingTill = System.currentTimeMillis() + timeout
        timeoutEvent = onTimeout( timeout ) { requestJoinFailed() }
      }
    }
  }

  private def requestJoinFailed() {
    if ( state == Waiting ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { requestJoinFailed() }
      } else {
        startElection()
      }
    }
  }

  private def haltFailed() {
    if ( state == Waiting ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { haltFailed() }
      } else { 
        startElection()
      }
    }
  }

  private def noPriorityRepliesForElection() {
    if ( state == ElectionPrep ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { noPriorityRepliesForElection() }
      } else {
        val timeout = definition.getTimeout
        updateState( Election )
        members = self :: potentialMembers filter { _ != self } filter { ! hasPriorityOverMe( _ ) }
        members filter { _ != self } foreach { _ ! Halt( self, electionSeq, members, sharedState ) }
        memberReplies = Nil
        waitingTill = System.currentTimeMillis() + timeout
        timeoutEvent = onTimeout( timeout ) { notAllLesserRepliedForElection() }
      }
    }
  }

  private def notAllLesserRepliedForElection() {
    if ( state == Election ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { notAllLesserRepliedForElection() }
      } else {
        val totalMembers = members.size
        val numReplied = memberReplies.size
        if ( numReplied * definition.quorumDenominator > totalMembers * definition.quorumNumerator ) {
          leader = self
          leaderChanged( true )
          updateState( Normal )
          members foreach { _ ! ImLeader( self, electionSeq, members, sharedState ) }
          val timeout = definition.getHeartbeatTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { heartBeat() }
        } else {
          // Election failed try again
          // Todo: degrade logics?
          updateState( ElectionPrep )
          potentialMembers filter { _ != self } foreach { member => if ( hasPriorityOverMe( member ) ) member ! AnyoneThere( self, state, leader, members ) }
          val timeout = definition.getTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { noPriorityRepliesForElection() }
        }
      }
    }
  }

  private def notAllLesserRepliedForUpdate() {
    if ( state == Election ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { notAllLesserRepliedForUpdate() }
      } else {
        val totalMembers = members.size
        val numReplied = memberReplies.size
        if ( numReplied * definition.quorumDenominator > totalMembers * definition.quorumNumerator ) {
          updateState( Normal )
          leader = self
          leaderChanged( true )
          members foreach { _ ! ImLeader( self, electionSeq, members, sharedState ) }
          val timeout = definition.getHeartbeatTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { heartBeat() }
        } else {
          // Election failed try again
          // Todo: degrade logics?
          updateState( ElectionPrep )
          potentialMembers filter { _ != self } foreach { member => if ( hasPriorityOverMe( member ) ) member ! AnyoneThere( self, state, leader, members ) }
          val timeout = definition.getTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { noPriorityRepliesForElection() }
        }
      }
    }
  }

  private def heartBeat() {
    if ( state == Normal ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { heartBeat() }
      } else {
        if ( ! isLeader ) {
          leader ! AnyoneThere( self, state, leader, members )
          val timeout = definition.getTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { heartBeatFailed() }
        } else {
          definition.requestPotentialMembers( this )
          val timeout = definition.getHeartbeatTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { heartBeat() }
        } 
      }
    }
  }

  private def heartBeatFailed() {
    if ( state == Normal ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { heartBeatFailed() }
      } else {
        // Begin an election
        val timeout = definition.getTimeout
        updateState( ElectionPrep )
        potentialMembers filter { _ != self } foreach { member => if ( hasPriorityOverMe( member ) ) member ! AnyoneThere( self, state, leader, members ) }
        waitingTill = System.currentTimeMillis() + timeout
        timeoutEvent = onTimeout( timeout ) { noPriorityRepliesForElection() }
      }
    }    
  }

  private def updateTimedOut() {
    if ( state == Normal ) {
      val currentTime = System.currentTimeMillis()
      if ( waitingTill > currentTime ) {
        timeoutEvent = onTimeout( waitingTill - currentTime ) { updateTimedOut() }
      } else {
        if ( ! isLeader ) {
          leader ! AnyoneThere( self, state, leader, members )
          val timeout = definition.getTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { heartBeatFailed() }
        } else {
          definition.requestPotentialMembers( this )
          val timeout = definition.getHeartbeatTimeout
          waitingTill = System.currentTimeMillis() + timeout
          timeoutEvent = onTimeout( timeout ) { heartBeat() }
        }
      }
    }
  }

  val groupReact: PartialFunction[Any, Unit] = {
    case AnyoneThere( them, theirState, theirLeader, theirMembers ) =>
      println( "AnyoneThere to " + self + " from " + them )
      reply( ImHere( self, state, leader, members ) )
      if ( ! ( potentialMembers contains them ) )
        potentialMembers ::= them
    case ImHere( them, theirState, theirLeader, theirMembers ) =>
      println( "ImHere to " + self + " from " + them )
      state match {
        case Initializing =>
          if ( ! ( potentialMembers contains them ) )
            potentialMembers ::= them
        case Singleton =>
        case ElectionPrep =>
          if ( them != self && hasPriorityOverMe( them ) && ( potentialMembers contains them ) ) {
            aPriorityRepliedForElection( them )
          }
        case Election =>
        case Normal =>
        case Waiting =>
        case _ => throw new RuntimeException( "What Happened!" )
      }
    case Halt( someLeader, someSeq, someMembers, someGroupState ) =>
      println( "Halt to " + self + " from " + someLeader )
      if ( someLeader != self && hasPriorityOverMe( someLeader ) ) {
        state match {
          case Initializing =>
          case Singleton =>
          case ElectionPrep =>
          case Election =>
            members foreach { _ ! Halt( someLeader, someSeq, someMembers, someGroupState ) }
          case Normal =>
            if ( isLeader )
              members foreach { _ ! Halt( someLeader, someSeq, someMembers, someGroupState ) }
          case Waiting =>
          case _ => throw new RuntimeException( "What Happened!" )
        }
        val timeout = definition.getTimeout
        updateState( Waiting )
        leader = someLeader
        leaderChanged( false )
        haltSeq = someSeq 
        waitingTill = System.currentTimeMillis() + timeout
        if ( timeoutEvent != null ) timeoutEvent.cancel()
        timeoutEvent = onTimeout( timeout ) { haltFailed() }
        reply( AcknowledgeHalt( self, someLeader, someSeq, true ) )
      } else {
        reply( AcknowledgeHalt( self, someLeader, someSeq, false ) )
      }
    case AcknowledgeHalt( member, targetLeader, targetSeq, accepted ) =>
      println( "AcknowledgeHalt to " + self + " from " + member )
      // todo: reason about acknowledge halt message
      state match {
        case Initializing =>
        case Singleton =>
        case ElectionPrep =>
        case Election =>
          if ( isLeader && leader == targetLeader && electionSeq == targetSeq && accepted ) {
            if ( ( members contains member ) && ! ( memberReplies contains member ) ) {
              memberReplies ::= member
              if ( members.size == memberReplies.size + 1 ) {
                updateState( Normal )
                val timeout = definition.getHeartbeatTimeout
                waitingTill = System.currentTimeMillis() + timeout
                if ( timeoutEvent != null ) timeoutEvent.cancel()
                timeoutEvent = onTimeout( timeout ) { heartBeat() }
              }
            }
          }
        case Normal =>
        case Waiting =>
        case _ => throw new RuntimeException( "What Happened!" )
      }
    case ImLeader( someLeader, someSeq, someMembers, someGroupState ) =>
      println( "ImLeader to " + self + " from " + someLeader )
      // todo: reason about this
      if ( someLeader != self && hasPriorityOverMe( someLeader ) ) {
        state match {
          case Initializing | Singleton | ElectionPrep | Waiting =>
            updateState( Normal )
            leader = someLeader
          case Election =>
            members foreach { _ ! Halt( someLeader, someSeq, someMembers, someGroupState ) }
            updateState( Normal )
            leader = someLeader
          case Normal =>
            if ( isLeader ) {
              members foreach { _ ! Halt( someLeader, someSeq, someMembers, someGroupState ) }
              updateState( Normal )
            }
            leader = someLeader
          case _ => throw new RuntimeException( "What Happened!" )
        }
      }
    case BeginUpdate( someLeader, someSeq, someMembers, someGroupState ) =>
      println( "BeginUpdate to " + self + " from " + someLeader )
      if ( someLeader._getUUID == leader._getUUID && state == Normal ) {
        val timeout = definition.getTimeout
        updateState = ReceivingUpdate
        receivedUpdateSeq = someSeq
        newGroupState = someGroupState
        newGroupStateTime = System.currentTimeMillis()
        waitingTill = System.currentTimeMillis() + timeout
        if ( timeoutEvent != null ) timeoutEvent.cancel()
        timeoutEvent = onTimeout( timeout ) { updateTimedOut() }
      }
    case AcknowledgeUpdate( member, targetLeader, targetSeq, accepted ) =>
      println( "AcknowledgeUpdate to " + self + " from " + member )
      if ( isLeader && targetLeader._getUUID == leader._getUUID && state == Normal && updateSeq == targetSeq && updateState == PushingUpdate ) {
        if ( ( members contains member ) && ! ( memberReplies contains member ) ) {
          if ( memberReplies.size + 1 == members.size ) {
            updateState = Stable
            sharedState = newGroupState
            sharedStateChanged( sharedState )
            newGroupState = null
            newGroupStateTime = 0
            val timeout = definition.getHeartbeatTimeout
            waitingTill = System.currentTimeMillis() + timeout
            if ( timeoutEvent != null ) timeoutEvent.cancel()
            timeoutEvent = onTimeout( timeout ) { heartBeat() }
            members foreach { _ ! CommitUpdate( self, updateSeq, members, sharedState ) }
          }
        }
      }
    case CommitUpdate( someLeader, someSeq, someMembers, someGroupState ) =>
      println( "CommitUpdate to " + self + " from " + someLeader )
      if ( someLeader._getUUID == leader._getUUID && state == Normal && receivedUpdateSeq == someSeq && updateState == ReceivingUpdate ) {
        updateState = Stable
        sharedState = newGroupState
        sharedStateChanged( sharedState )
        newGroupState = null
        newGroupStateTime = 0
        val timeout = definition.getHeartbeatTimeout
        waitingTill = System.currentTimeMillis() + timeout
        if ( timeoutEvent != null ) timeoutEvent.cancel()
        timeoutEvent = onTimeout( timeout ) { heartBeat() }
      }
    case CancelUpdate( someLeader, someSeq, someMembers ) =>
      println( "CancelUpdate to " + self + " from " + someLeader )
      if ( someLeader._getUUID == leader._getUUID && state == Normal && receivedUpdateSeq == someSeq && updateState == ReceivingUpdate ) {
        updateState = Stable
        newGroupState = null
        newGroupStateTime = 0
        val timeout = definition.getHeartbeatTimeout
        waitingTill = System.currentTimeMillis() + timeout
        if ( timeoutEvent != null ) timeoutEvent.cancel()
        timeoutEvent = onTimeout( timeout ) { heartBeat() }
      }
    case JoinRequest( possibleMember ) =>
      println( "JoinRequest to " + self + " from " + possibleMember )
      state match {
        case Initializing =>
        case Singleton =>
        case ElectionPrep =>
        case Election =>
        case Normal => {
          if ( isLeader && possibleMember != self && ! hasPriorityOverMe( possibleMember ) ) {
            val timeout = definition.getTimeout
            updateState( Election )
            members = if ( members contains possibleMember ) members else possibleMember :: members
            members foreach { _ ! Halt( self, electionSeq, members, sharedState ) }
            memberReplies = Nil
            waitingTill = System.currentTimeMillis() + timeout
            if ( timeoutEvent != null ) timeoutEvent.cancel()
            timeoutEvent = onTimeout( timeout ) { notAllLesserRepliedForElection() }
          }
        }
        case Waiting =>
        case _ => throw new RuntimeException( "What Happened!" )
      }
    case UpdateRequest( member, requestedGroupState ) =>
      println( "UpdateRequest to " + self + " from " + member )
      state match {
        case Initializing =>
        case Singleton =>
        case ElectionPrep =>
        case Election =>
        case Normal => {
          if ( isLeader && ( members contains member ) ) {
            updateState match {
              case Stable => beginGroupUpdate( requestedGroupState )
              case PushingUpdate =>
              case ReceivingUpdate => throw new RuntimeException( "What Happened!" )
              case _ => throw new RuntimeException( "What Happened!" )
            }
          }
        }
        case Waiting =>
        case _ => throw new RuntimeException( "What Happened!" )
      }
  }

  private def beginGroupUpdate(requestedGroupState:HashMap[String,Any]) {
    val timeout = definition.getTimeout
    updateSeq += 1
    updateState = PushingUpdate
    newGroupState = requestedGroupState
    newGroupStateTime = System.currentTimeMillis()
    members foreach { _ ! BeginUpdate( self, updateSeq, members, newGroupState ) }
    memberReplies = Nil
    waitingTill = System.currentTimeMillis() + timeout
    if ( timeoutEvent != null ) timeoutEvent.cancel()
    timeoutEvent = onTimeout( timeout ) { notAllLesserRepliedForUpdate() }
  }
}
