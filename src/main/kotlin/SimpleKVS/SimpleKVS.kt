package SimpleKVS

import java.io.RandomAccessFile




fun main() {
    val raf = RandomAccessFile("data/simplekvs/database.txt", "rw")
    raf.writeBytes("test")
    raf.close()


}

class SimpleKVS {

}

