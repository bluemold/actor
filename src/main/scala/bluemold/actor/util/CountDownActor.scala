package bluemold.actor.util

import annotation.tailrec
import bluemold.actor.{ReplyChannel, SimpleActor, ActorStrategy}

/**
 * CountDownActor<br/>
 * Author: Neil Essy<br/>
 * Created: 5/26/11<br/>
 * <p/>
 * [Description]
 */

class CountDownActor( initial: Int )( implicit _strategy: ActorStrategy ) extends SimpleActor()( _strategy ) {
  var awaiting: List[ReplyChannel] = _
  var count: Int = _

  protected def init() {
    awaiting = Nil
    count = initial
  }

  @tailrec
  private final def notify( awaiting: List[ReplyChannel] ) {
    awaiting match {
      case head :: tail => {
        head.issueReply( "wakeup" )
        notify( tail )
      }
      case Nil => // done
    }
  }

  protected def react: PartialFunction[Any, Unit] = {
    case "countDown" => {
      if ( count > 0 ) {
        count -= 1
        if ( count == 0 ) {
          notify( awaiting )
          awaiting = Nil // be gc friendly
        }
      }
    } 
    case "getCount" => reply( count )
    case "await" => {
      if ( count == 0 ) reply( "wakeup" )
      else awaiting ::= sender
    }
  }
}