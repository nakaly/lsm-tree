package lsm.statistics

import lsm.sstable.Logs
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.FileWriter

class Statistics(val statisticsFile: File, var nextSequenceNo: Int, val activeSequenceNo: MutableList<Int>) {

    private fun formatStatistics(nextSequenceNo: Int, activeSequenceNo: Sequence<Int>): String {
        return "$nextSequenceNo ${activeSequenceNo.joinToString(",", "[", "]")}"
    }

    fun updateStatistics(nextSequenceNo: Int, logs: Logs) {
        if (!statisticsFile.exists()) {
            throw IllegalStateException("files not found: ${statisticsFile.absolutePath}")
        }
        val writer = FileWriter(statisticsFile)
        try {
            val formatted = formatStatistics(nextSequenceNo, logs.activeSequenceNo())
            writer.write(formatted)
            writer.flush()
        } finally {
            writer.close()
        }
        activeSequenceNo.add(this.nextSequenceNo)
        this.nextSequenceNo = nextSequenceNo
    }


    companion object {
        fun initialize(statisticsFilePath: String): Statistics {
            val file = File(statisticsFilePath)
            if (!file.exists()) {
                if (!file.createNewFile()) {
                    throw IllegalStateException("files not found: ${statisticsFilePath}")
                }
                val writer = FileWriter(file)
                try {
                    writer.write("0 []")
                    writer.flush()
                } finally {
                    writer.close()
                }
            }

            val reader = BufferedReader(FileReader(file))
            try {
                val statistics = reader.readLine().split("\\s+".toRegex())
                val nextSequenceNo = statistics[0].toInt()
                val activeSequenceNo =
                    statistics[1]
                        .substring(1 until statistics[1].length - 1)
                        .split(",")
                        .filter { !it.isBlank() }
                        .map { it.toInt() }

                return Statistics(file, nextSequenceNo, activeSequenceNo.toMutableList())
            } finally {
                reader.close()
            }
        }

    }
}