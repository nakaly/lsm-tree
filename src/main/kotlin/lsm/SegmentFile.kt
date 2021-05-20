package lsm

import java.io.File
import java.io.RandomAccessFile

class SegmentFile(val sequenceNo: Int, basePath: String) {
    private val filePath = "$basePath/segment_file_$sequenceNo.txt"


    fun raf(mode: String): RandomAccessFile {
        val file = File(filePath)
        return RandomAccessFile(file, mode)
    }

    fun newReadOnlyRaf(): RandomAccessFile {
        return raf("r")
    }

    fun newReadWriteRaf(): RandomAccessFile {
        return raf("rw")
    }

    fun createSegmentFile(): Boolean {
        val file = File(filePath)
        if (file.exists()) {
            throw IllegalStateException("lready exist segment file: $filePath")
        }
        return file.createNewFile()
    }

    companion object {
        val KEY_HEADER_SIZE = Integer.BYTES
        val VALUE_HEADER_SIZE = Integer.BYTES
        val HEADER_SIZE = Integer.BYTES * 2
        val MIN_DATA_SIZE = HEADER_SIZE + 2
        val TOMBSTONE = Integer.MIN_VALUE
    }
}