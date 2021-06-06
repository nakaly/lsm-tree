package lsm.statistics

import lsm.sstable.Logs
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.FileWriter

class Statistics(val statisticsFile: File, val nextSequenceNo: Int, val activeSequenceNo: List<Int>) {

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
                val statistics = reader.readLine().split("\\s+")
                val nextSequenceNo = statistics[0].toInt()
                val activeSequenceNo =
                    statistics[1]
                        .split(",")
                        .filter { it.isBlank() }
                        .map { it.toInt() }

                return Statistics(file, nextSequenceNo, activeSequenceNo)
            } finally {
                reader.close()
            }
        }

    }
}