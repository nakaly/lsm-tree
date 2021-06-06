package lsm

import lsm.sstable.Log
import lsm.sstable.Logs
import lsm.sstable.SSTableFactory
import lsm.sstable.WriteAheadLog
import lsm.statistics.Statistics

fun main() {

}

class LSMTree(
    statistics: Statistics,
    memTable: Log.MemTable,
    logs: Logs,
    writeAheadLog: WriteAheadLog,
    ssTableFactory: SSTableFactory
) {
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