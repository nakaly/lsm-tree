package lsm

import lsm.sstable.*
import lsm.statistics.Statistics

fun main() {

}

class LSMTree(
    val statistics: Statistics,
    val memTable: Log.MemTable,
    val logs: Logs,
    val writeAheadLog: WriteAheadLog,
    val ssTableFactory: SSTableFactory
) {

    fun set(key: String, value: String): Unit {
        memTable.set(key, value)
        if (memTable.isOverMaxSize()) {
            val next = statistics.nextSequenceNo
            logs.updated(
                next,
                Log.SSTableRef(ssTableFactory.apply(statistics.nextSequenceNo, memTable.iterator()))
            )
        }
    }

    fun get(key: String): SegmentFileReadable.Got {
        return when (memTable.get(key)) {
            SegmentFileReadable.NotFound -> logs.read(key)
            else -> memTable.get(key)
        }
    }

    companion object {
        fun initialize(
            statisticsFilePath: String,
            segmentFileBathPath: String,
            writeAheadLogFilePath: String
        ): LSMTree {
            val statistics = Statistics.initialize(statisticsFilePath)
            val writeAheadLog = WriteAheadLog.initialize(writeAheadLogFilePath)
            val memTable: Log.MemTable = writeAheadLog.recovery()
            val logs = Logs()

            statistics.nextSequenceNo
            val ssTableFactory = SSTableFactory(5, segmentFileBathPath)

            statistics.activeSequenceNo.forEach {
                logs.updated(it, Log.SSTableRef(ssTableFactory.recovery(it)))
            }
            return LSMTree(statistics, memTable, logs, writeAheadLog, ssTableFactory)
        }
    }
}