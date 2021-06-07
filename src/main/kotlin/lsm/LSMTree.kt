package lsm

import lsm.sstable.*
import lsm.statistics.Statistics
import java.util.*

fun main() {
    val lsmTree = LSMTree.initialize(
        "data/lsmtree/statistics",
        "data/lsmtree",
        "data/lsmtree/wal"
    )
    lsmTree.set("1", "foo")
    lsmTree.set("2", "bar")
    lsmTree.set("3", "foobar")
    lsmTree.set("4", "dump")
    val got = lsmTree.get("1")
    println(got)

}

class LSMTree(
    val statistics: Statistics,
    var memTable: Log.MemTable,
    var logs: Logs,
    val writeAheadLog: WriteAheadLog,
    val ssTableFactory: SSTableFactory
) {

    fun set(key: String, value: String): Unit {
        memTable.set(key, value)
        writeAheadLog.set(key, value)
        if (memTable.isOverMaxSize()) {
            val next = statistics.nextSequenceNo
            logs = logs.updated(
                next,
                Log.SSTableRef(ssTableFactory.apply(statistics.nextSequenceNo, memTable.iterator()))
            )
            statistics.updateStatistics(next + 2, logs)
            memTable = Log.MemTable(TreeMap<String, SegmentFileReadable.Value>())
            writeAheadLog.clear()
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