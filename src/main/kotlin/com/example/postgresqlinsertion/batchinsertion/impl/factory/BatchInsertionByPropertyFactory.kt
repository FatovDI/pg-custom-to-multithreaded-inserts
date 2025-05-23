package com.example.postgresqlinsertion.batchinsertion.impl.factory

import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByPropertyFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.SaverType
import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByPropertySaver
import com.example.postgresqlinsertion.batchinsertion.impl.saver.*
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import org.springframework.beans.factory.annotation.Value
import javax.sql.DataSource
import kotlin.reflect.KClass

abstract class BatchInsertionByPropertyFactory<E: BaseEntity>(
    private val entityClass: KClass<E>,
    override val processor: BatchInsertionByPropertyProcessor,
    private val dataSource: DataSource,
) : BatchInsertionByPropertyFactory<E> {

    @Value("\${batch_insertion.batch_size}")
    private var batchSize: Int = 100

    override fun getSaver(type: SaverType): BatchInsertionByPropertySaver<E> {

        val conn = dataSource.connection

        return when (type) {
            SaverType.COPY -> CopyByPropertySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_BINARY -> CopyBinaryByPropertySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_VIA_FILE -> CopyViaFileByPropertySaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_BINARY_VIA_FILE -> CopyBinaryViaFileByPropertySaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT -> InsertByPropertySaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_MULTI_ROW -> InsertByPropertyMultiRowSaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_PREPARED_STATEMENT -> InsertByPropertyPSSaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_PREPARED_STATEMENT_MULTI_ROW -> InsertByPropertyMultiRowPSSaver(processor, entityClass, conn, batchSize)
            SaverType.INSERT_PREPARED_STATEMENT_UNNEST -> InsertByPropertyPSUnnestSaver(processor, entityClass, conn, batchSize)
            SaverType.UPDATE -> UpdateByPropertySaver(processor, entityClass, conn, batchSize)
            SaverType.UPDATE_PREPARED_STATEMENT -> UpdateByPropertyPreparedStatementSaver(processor, entityClass, conn, batchSize)
            SaverType.COPY_CONCURRENT -> TODO()
            SaverType.COPY_BINARY_CONCURRENT -> TODO()
            SaverType.INSERT_PS_MR_CONCURRENT_ATOMIC -> TODO()
            SaverType.COPY_BINARY_CONCURRENT_ATOMIC -> TODO()
        }

    }
}
