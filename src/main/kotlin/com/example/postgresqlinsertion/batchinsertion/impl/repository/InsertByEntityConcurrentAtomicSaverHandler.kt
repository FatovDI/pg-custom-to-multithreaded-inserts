package com.example.postgresqlinsertion.batchinsertion.impl.repository

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByEntitySaver
import com.example.postgresqlinsertion.batchinsertion.impl.saver.InsertByEntityConcurrentAtomicSaver
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.util.UUID
import java.util.concurrent.ExecutorService
import javax.sql.DataSource
import kotlin.reflect.KClass

class InsertByEntityConcurrentAtomicSaverHandler<E : BaseEntity>(
    private val processor: BatchInsertionByEntityProcessor,
    private val entityClass: KClass<E>,
    private val dataSource: DataSource,
    private val batchSize: Int,
    private val numberOfSavers: Int = 4,
    executorService: ExecutorService
): BatchInsertionByEntitySaver<E> {
    private var counterEntity = 0
    private var counterSaver = 0
    private val savers = (1..numberOfSavers)
        .map {
            val conn = dataSource.connection
            conn.autoCommit = false
            InsertByEntityConcurrentAtomicSaver(processor, entityClass, dataSource.connection, batchSize, executorService, UUID.randomUUID())
        }

    override fun addDataForSave(entity: E) {

        val currSaver = savers[counterSaver % numberOfSavers]

        currSaver.addDataForSave(entity)

        counterEntity++
        counterEntity.takeIf { it % batchSize == 0 }?.let { counterSaver++ }
    }

    override fun saveData() {
        savers.forEach {
            it.saveData()
        }
    }

    override fun commit() {
        val conn = dataSource.connection
        savers
            .joinToString(", ") {
                it.commit()
                "'${it.transactionId}'"
            }
            .let {
                conn.createStatement().use { stmt ->
                    stmt.execute("COMMIT PREPARED $it;")
                }
                conn.close()
            }
    }

    override fun rollback() {
        savers.forEach {
            it.rollback()
        }
    }

    override fun close() {
        savers.forEach {
            it.close()
        }
    }
}
