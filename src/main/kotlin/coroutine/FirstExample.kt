package coroutine

import io.reactivex.rxjava3.kotlin.subscribeBy
import io.reactivex.rxjava3.kotlin.toObservable
import io.reactivex.rxjava3.schedulers.Schedulers
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.IOException
import java.lang.RuntimeException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis


fun log(msg: String) = println("[${Thread.currentThread().name}] $msg")
val threadLocal = ThreadLocal<String?>()

val counterContext = newSingleThreadContext("CounterContext")
val mutex = Mutex()
var counter = 0

fun main() {
    val (h, w) = readLine()!!.split(' ').map { it.toInt() }
    val r = if (h < w ) h * h else w * w
    println(r)

//    val (h, w) =
//    val r = if (h < w ) h * h else w * w
//    print(r)

//    val list = listOf("Alpha", "Beta", "Gamma", "Delta", "Epsilon")
//    runBlocking {
//        flow {
//            for (i in list) {
//                Thread.sleep(100)
//                log("Emitting $i")
//                emit(i) // emit next value
//            }
//        }.flowOn(Dispatchers.IO)
//                .collect { value -> log("Collected $value") }
//    }

//    runBlocking {
//        CoroutineScope(Job() + Dispatchers.Default).launch {
//            flowOf(list)
//                    .onEach { println(" on thread: ${Thread.currentThread().name}") }
//                    .flowOn(Dispatchers.IO)
//                    .onEach { println("collect on thread: ${Thread.currentThread().name}") }
//                    .launchIn(this)
//        }
//    }
//
//    list.toObservable()
//            .doOnNext { word -> run { log("run") } }
//            .subscribeOn(Schedulers.io())
//            .observeOn(Schedulers.computation())
//            .subscribe { result -> log("result: $result") }
//    Thread.sleep(1000)
}

private suspend fun doWorkDelay(i: Int, timeout: Long): Int {
    delay(timeout)
    return i
}

private fun flow(
        timeout: Long,
        limit: Int
): Flow<Int> = flow {
    for (i in 1..limit) {
        delay(timeout)
        emit(i)
    }
}

private fun testFlow(limit: Int = 10, onBackpressure: Flow<Int>.() -> Flow<Int>) {

    val latch = CountDownLatch(1)
    val time = System.currentTimeMillis()

    val stringBuffer = StringBuffer()

    CoroutineScope(Job() + Dispatchers.Default).launch {

        flow(timeout = 100, limit = limit)
                .flowOn(Dispatchers.IO)
                .onBackpressure()
                .map { doWorkDelay(i = it, timeout = 200) }
                .map { doWorkDelay(i = it, timeout = 300) }
                .onCompletion { latch.countDown() }
                .collect {
                    stringBuffer.append("$it ")
                }
    }

    latch.await()

    println((System.currentTimeMillis() - time))
    println(stringBuffer.toString())
}


fun CoroutineScope.counterActor() = actor<CounterMsg> {
    var counter = 0
    for (msg in channel) {
        when (msg) {
            is IncCounter -> {
                counter++
                println("counter increment $counter")
            }
            is GetCounter -> msg.response.complete(counter)
        }
    }
}

sealed class CounterMsg
object IncCounter : CounterMsg()
class GetCounter(val response: CompletableDeferred<Int>) : CounterMsg()

suspend fun massiveRun(action: suspend () -> Unit) {
    val n = 100
    val k = 1000
    val time = measureTimeMillis {
        coroutineScope {
            repeat(n) {
                launch {
                    repeat(k) { action() }
                }
            }
        }
    }
    println("Completed ${n * k} actions in $time ms")
}


suspend fun player(name: String, table: Channel<Ball>) {
    for (ball in table) {
        ball.hits++
        println("$name $ball")
        delay(300)
        table.send(ball)
    }
}

data class Ball(var hits: Int)

suspend fun sendString(channel: SendChannel<String>, s: String, time: Long) {
    while (true) {
        delay(time)
        channel.send(s)
    }
}

fun CoroutineScope.produceSquares(): ReceiveChannel<Int> = produce {
    for (x in 1..5) send(x * x)
}

fun CoroutineScope.produceNumbers() = produce<Int> {
    var x = 1
    while (true) {
        send(x++)
        delay(100)
    }
}

fun CoroutineScope.launchProcessor(id: Int, channel: ReceiveChannel<Int>) = launch {
    for (msg in channel) {
        println("${java.lang.Thread.currentThread()}")
        println("Processor #$id received $msg")
    }
}

fun CoroutineScope.square(numbers: ReceiveChannel<Int>): ReceiveChannel<Int> = produce {
    for (x in numbers) send(x * x)
}


fun simple(): Flow<Int> = flow {
    for (i in 1..3) {
        Thread.sleep(100) // pretend we are computing it in CPU-consuming way
        log("Emitting $i")
        emit(i) // emit next value
    }
}

fun numbers(): Flow<Int> = flow {
    try {
        emit(1)
        emit(2)
        println("This line will not execute")
        emit(3)
    } finally {
        println("Finally in numbers")
    }
}

fun CoroutineScope.numbersFrom(start: Int) = produce<Int> {
    var x = start
    while (true) {
        send(x++)
    }
}

fun CoroutineScope.filter(numbers: ReceiveChannel<Int>, prime: Int) = produce<Int> {

    for (x in numbers) {
        println("in filter x ($x) % prime($prime) =  ${x % prime}")
        if (x % prime != 0) {
            println("in filter if sendding: $x")
            send(x)
        }
    }
}


suspend fun performRequest(request: Int): String {
    delay(1000) // imitate long-running asynchronous work
    return "response $request"
}


class Activity {
    private val mainScope = CoroutineScope(Dispatchers.Default) //

    fun destroy() {
        mainScope.cancel()
    }

    fun doSomething() {
        repeat(10) { i ->
            mainScope.launch {
                delay((i + 1) * 200L)
                println("Coroutine $i is done")
            }
        }
    }
}


suspend fun failedConcurrentSum(): Int = coroutineScope {
    val one = async<Int> {
        try {
            delay(Long.MAX_VALUE)
            42
        } finally {
            println("First child was canceld")
        }

    }
    val two = async<Int> {
        println("Second child throws an exception")
        throw ArithmeticException()
    }
    one.await() + two.await()
}

suspend fun doSomethingUsefulOne(): Int {
    delay(1000L)
    return 13
}

suspend fun doSomethingUsefulTwo(): Int {
    delay(1000L)
    return 29
}

class FirstExample {

}