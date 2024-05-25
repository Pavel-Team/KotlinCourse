import kotlinx.coroutines.*
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

interface ParallelDeferred<out T>{
    suspend fun await(): T
    suspend fun join()
}

class ParallelDeferredImpl<T>(
    context: CoroutineContext = EmptyCoroutineContext,
    private val dependencies: List<ParallelDeferred<*>> = emptyList(),
    private val block: suspend CoroutineScope.() -> T,
) : ParallelDeferred<T> {

    private var result: T? = null
    private val job = Job()
    private val scope = CoroutineScope(context)

    override suspend fun await(): T {
        if (!job.isCompleted) {
            scope.launch {
                runDependencies()
                result = block()

                job.complete()
            }
            job.join()
        }
        return result!!
    }

    override suspend fun join() {
        job.join()
    }

    private suspend fun runDependencies() {
        dependencies.forEach { dependency ->
            scope.launch {
                dependency.await()
            }
        }
        dependencies.forEach { dependency ->
            dependency.join()
        }
    }

}

fun <T> CoroutineScope.asyncParallel(
    context: CoroutineContext = EmptyCoroutineContext,
    dependencies: List<ParallelDeferred<*>> = emptyList(),
    block: suspend CoroutineScope.() -> T,
): ParallelDeferred<T> {
    return ParallelDeferredImpl(context, dependencies, block)
}


fun main() = runBlocking {
    val job1 = asyncParallel {
        println("job1 started")
        delay(500)
        println("job1 completed")
        return@asyncParallel 1
    }

    val job2 = asyncParallel {
        println("job2 started")
        delay(100)
        println("job2 completed")
        return@asyncParallel 2
    }

    val job3 = asyncParallel(dependencies = listOf(job1, job2)) {
        println("job3 started")

        println("start get res1")
        val res1 = job1.await()
        println("finish get res1")

        println("start get res2")
        val res2 = job2.await()
        println("finish get res2")
        return@asyncParallel res1 + res2
    }

    val result = job3.await()
    println("result = $result")
}