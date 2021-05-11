package SimpleKVS

import com.github.michaelbull.result.Err
import com.github.michaelbull.result.Ok
import com.github.michaelbull.result.Result
import java.io.IOException
import java.io.RandomAccessFile



fun main() {
//    val raf = RandomAccessFile("data/simplekvs/database.txt", "rw")
//    raf.writeBytes("test")
//    raf.close()
    val kvs = SimpleKVS("data/simplekvs/database.txt")
    kvs.set("key", "value")


}

class SimpleKVS(filePath: String) {
    private val raf = RandomAccessFile(filePath, "rw")
    private val keyIndex = mutableMapOf<String, Long>()

    fun set(key: String, value: String): Result<Unit, Exception> {
        val befPos = raf.length()
        try {
            raf.seek(raf.length())
            raf.writeBytes(value)
            raf.writeInt(value.length)
            raf.writeBytes(key)
            raf.writeInt(key.length)
        } catch (e: IOException) {
            raf.setLength(befPos)
            return Err(e)
        }
        keyIndex[key] = raf.filePointer
        return Ok(Unit)
    }

    fun get(key: String) {

    }

}

