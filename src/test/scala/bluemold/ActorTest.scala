package bluemold

import junit.framework._;
import Assert._
import bluemold.actor.Actor
import bluemold.actor.Actor._
import java.util.concurrent.CountDownLatch
;

object ActorTest {
    def suite: Test = {
        val suite = new TestSuite(classOf[ActorTest]);
        suite
    }

    def main(args : Array[String]) {
        junit.textui.TestRunner.run(suite);
    }
}

/**
 * Unit test for simple App.
 */
class ActorTest extends TestCase("actor") {

    /**
     * Rigourous Tests :-)
     */
    def testBasics() {
      val latch = new CountDownLatch(1) 
      val actor = actorOf( new Actor {

        protected def handleException(t: Throwable) { assertTrue( false ) }

        protected def react: PartialFunction[Any, Unit] = {
          case msg => latch.countDown() 
        }

        protected def init() {}

      }).start()
      
      actor ! "hi"
      
      latch.await()
    }
    

}
