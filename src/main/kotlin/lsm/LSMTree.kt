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
        fun initialize(statisticsFilePath: String, segmentFileBathPath: String, writeAheadLogFilePath: String) {
            Statistics.initialize("data/lsmtree")

        }
    }
}