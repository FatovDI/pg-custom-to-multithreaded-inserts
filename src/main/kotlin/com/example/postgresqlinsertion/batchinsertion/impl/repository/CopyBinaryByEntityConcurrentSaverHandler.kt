package com.example.postgresqlinsertion.batchinsertion.impl.repository

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByEntitySaver
import com.example.postgresqlinsertion.batchinsertion.impl.saver.CopyBinaryByEntityConcurrentSaver
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.util.concurrent.ExecutorService
import javax.sql.DataSource
import kotlin.reflect.KClass

class CopyBinaryByEntityConcurrentSaverHandler<E : BaseEntity>(
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
            CopyBinaryByEntityConcurrentSaver(
                processor,
                entityClass,
                dataSource.connection,
                batchSize,
                executorService)
        }

    override fun addDataForSave(entity: E) {

        val currSaver = savers[counterSaver % numberOfSavers]

        currSaver.addDataForSave(entity)

        counterEntity++
        counterEntity.takeIf { it % batchSize == 0 }?.let { counterSaver++ }
    }

    override fun commit() {
        savers.forEach {
            it.commit()
        }
    }

    override fun saveData() {
        savers.forEach {
            it.saveData()
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
