package com.example.postgresqlinsertion.batchinsertion.impl.factory

import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByEntityFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.SaverType
import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByEntitySaver
import com.example.postgresqlinsertion.batchinsertion.impl.repository.CopyBinaryByEntityConcurrentSaverHandler
import com.example.postgresqlinsertion.batchinsertion.impl.repository.CopyByEntityConcurrentSaverHandler
import com.example.postgresqlinsertion.batchinsertion.impl.saver.*
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import java.util.concurrent.Executors
import javax.sql.DataSource
import kotlin.reflect.KClass

abstract class BatchInsertionByEntityFactory<E: BaseEntity>(
    private val entityClass: KClass<E>,
) : BatchInsertionByEntityFactory<E> {

    @Value("\${batch_insertion.batch_size}")
    private var batchSize: Int = 100

    @Value("\${batch_insertion.pool_size}")
    private var poolSize: Int = 4

    @Value("\${batch_insertion.concurrent_saves}")
    private var concurrentSavers: Int = 1

    @Autowired
    override lateinit var processor: BatchInsertionByEntityProcessor

    @Autowired
    override lateinit var dataSource: DataSource

    private val executorService by lazy {
        Executors.newFixedThreadPool(poolSize)
    }

    override fun getSaver(type: SaverType): BatchInsertionByEntitySaver<E> {

        val conn = dataSource.connection

        return when (type) {
            SaverType.COPY -> CopyByEntitySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_BINARY -> CopyBinaryByEntitySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_VIA_FILE -> CopyViaFileByEntitySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_BINARY_VIA_FILE -> CopyBinaryViaFileByEntitySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_CONCURRENT -> CopyByEntityConcurrentSaverHandler(processor, entityClass, dataSource, batchSize, concurrentSavers, executorService)
            SaverType.COPY_BINARY_CONCURRENT -> CopyBinaryByEntityConcurrentSaverHandler(processor, entityClass, dataSource, batchSize, concurrentSavers, executorService)
            SaverType.INSERT -> InsertByEntitySaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_MULTI_ROW -> InsertByEntityMultiRowSaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_PREPARED_STATEMENT -> InsertByEntityPSSaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_PREPARED_STATEMENT_MULTI_ROW -> InsertByEntityMultiRowPSSaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_PREPARED_STATEMENT_UNNEST -> InsertByEntityPSUnnestSaver(processor, entityClass, conn, batchSize)
            SaverType.UPDATE -> UpdateByEntitySaver(processor, entityClass, conn, batchSize)
            SaverType.UPDATE_PREPARED_STATEMENT -> UpdateByEntityPreparedStatementSaver(processor, entityClass, conn, batchSize)
        }

    }
}
