package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import java.util.*
import java.util.concurrent.ExecutorService
import kotlin.reflect.KClass

class CopyBinaryByEntityConcurrentAtomicSaver<E : BaseEntity>(
    processor: BatchInsertionByEntityProcessor,
    entityClass: KClass<E>,
    conn: Connection,
    batchSize: Int,
    executorService: ExecutorService,
    val transactionId: UUID,
) : CopyBinaryByEntityConcurrentSaver<E>(processor, entityClass, conn, batchSize, executorService) {

    override fun commit() {
        checkSaveDataJob()
        super.saveData()
        conn.createStatement().use { stmt ->
            stmt.execute("PREPARE TRANSACTION '$transactionId';")
        }
        conn.commit()
    }
}