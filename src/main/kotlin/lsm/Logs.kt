package lsm

import kotlin.streams.asSequence

class Logs(var underlying: Map<Int, Log>) {

    fun updated(sequenceNo: Int, log: Log): Logs {
        return Logs(underlying + (sequenceNo to log))
    }

    fun activeSequenceNo(): Sequence<Int> {
        return underlying.values.stream()
            .map {
                when (it) {
                    is Log.SSTableRef -> it.sStable.sequenceNo
                    is Log.MemTable -> -1
                }
            }.filter { it >= 0 }
            .asSequence()
    }


    fun read(key: String): SegmentFileReadable.Got {
        underlying.values.forEach {
            val value = when (it) {
                is Log.MemTable -> it.get(key)
                is Log.SSTableRef -> it.sStable.newReader().get(key)
            }
            return value
        }
        return SegmentFileReadable.NotFound
    }
}