package lsm

import java.util.*

class Logs(var underlying: SortedMap<Int, Log>) {

    fun updated(sequenceNo: Int, log: Log): Logs {
        underlying.put(sequenceNo, log)
        return Logs(underlying)
    }

    fun activeSequenceNo(): Sequence<Int> {
        val temp = underlying.values
            .map {
                when (it) {
                    is Log.SSTableRef -> it.sStable.sequenceNo
                    is Log.MemTable -> -1
                }
            }.filter { it >= 0 }
        return temp.asSequence()
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

    constructor() : this(TreeMap<Int, Log>(Comparator.reverseOrder()))

}

