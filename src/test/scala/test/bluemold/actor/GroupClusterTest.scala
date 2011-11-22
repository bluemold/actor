package test.bluemold.actor

import junit.framework._
import bluemold.actor.Actor._
import java.util.concurrent.CountDownLatch
import bluemold.actor._
import collection.immutable.HashMap
import bluemold.actor.GroupActor.GroupActorState

object GroupClusterTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[ClusterTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for actor clustering.
 */
class GroupClusterTest extends TestCase("cluster") {

  val latchOne = new CountDownLatch(1)
  val latchTwo = new CountDownLatch(1)

  def testBasics() {
    val fred = clusterOne()
    val george = clusterTwo()

    fred ! "announce"
    george ! "announce"
    
    latchOne.await()
    latchTwo.await()
    
    synchronized { wait( 10000 ) }
    fred.stop()
    george.stop()
    println( "Actors Stopped!")
    synchronized { wait( 10000 ) }
  }
  
  def clusterOne(): ActorRef = {
    implicit val cluster = UDPCluster.getCluster( "clusterOne", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new GroupClusterActor( "Fred", latchOne ) ).start()
  }
    
  def clusterTwo(): ActorRef = {
    implicit val cluster = UDPCluster.getCluster( "clusterTwo", "default" )
    implicit val strategyFactory = new FiberStrategyFactory()
    actorOf( new GroupClusterActor( "George", latchTwo) ).start()
  }

  class GroupClusterActor( name: String, latch: CountDownLatch ) extends GroupActor {
    var count: Int = _

    override protected def getId = "test"

    val myDefinition = new GroupActor.AllGroup( getId )

    def definition = myDefinition

    protected def init() {
      initGroupActor()
      count = 10
    }
    val myReact: PartialFunction[Any, Unit] = {
      case "announce" => {
        println( getCluster )
        getCluster.sendAll( classOf[GroupClusterActor], name )
      }
      case msg: String => {
        if ( msg != name ) {
          if ( count > 0 ) {
            count -= 1
            println( name + " count: " + count + " heard: " + msg )
            if ( count > 0 ) {
              reply( name )
            } else {
              reply( name )
              latch.countDown()
            }
          } else println( name + " heard extra: " + msg )
        } else println( name + " heard echo: " + msg )
      }
    }
    val combinedReact = groupReact orElse myReact
    protected def react: PartialFunction[Any, Unit] = combinedReact

    protected def stateChanged(state: GroupActorState) { println( name + ": State Changed: " + state ) }

    protected def sharedStateChanged(groupState: HashMap[String, Any]) { println( name + ": Shared State changed!" ) }

    protected def leaderChanged(iamleader: Boolean) { println( name + ( if ( iamleader ) ": I am the leader" else ": I am no longer the leader" ) ) }
  }
}
