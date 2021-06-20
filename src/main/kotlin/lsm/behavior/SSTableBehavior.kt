package lsm.behavior


import akka.actor.typed.*
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Routers
import lsm.sstable.SSTable
import lsm.sstable.SegmentFileReadable
import scala.concurrent.duration.Duration

class SSTableBehavior {

    class Get(val key: String, val replyTo: ActorRef<SegmentFileReadable.Got>)

    fun pool(sSTable: SSTable, size: Int): Behavior<Get> {
        return Routers.pool(
            size,
            Behaviors
                .supervise(worker(sSTable))
                .onFailure(
                    SupervisorStrategy.restart()
                        .withLimit(3, Duration.create(3, "seconds"))
                )
        )
    }

    private fun worker(sSTable: SSTable): Behavior<Get> {
        return Behaviors.setup {
            val sSTableReader = sSTable.newReader()
            return@setup MyBehavior(sSTableReader)
        }
    }


    class MyBehavior(private val sStableReader: SSTable.SSTableReader) : ExtensibleBehavior<Get>() {

        @Override
        override fun receive(ctx: TypedActorContext<Get>?, msg: Get?): Behavior<Get> {
            msg!!.replyTo.tell(sStableReader.get(msg.key))
            return Behaviors.same()
        }

        @Override
        override fun receiveSignal(ctx: TypedActorContext<Get>?, msg: Signal?): Behavior<Get> {
            if (msg is PreRestart || msg is PostStop) {
                sStableReader.close()
                return Behaviors.same()
            }
            return Behaviors.unhandled()
        }

    }


}


