package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.getDataFromEntity
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import kotlin.reflect.KClass

open class InsertByEntityPreparedStatementSaver<E: BaseEntity>(
    private val processor: BatchInsertionByEntityProcessor,
    private val entityClass: KClass<E>,
    conn: Connection,
    batchSize: Int
) : AbstractBatchInsertionByEntitySaver<E>(conn, batchSize) {

    private val dataForInsert = mutableListOf<List<Any?>>()

    override fun addDataForSave(entity: E) {
        dataForInsert.add(getDataFromEntity(entity))
        super.addDataForSave(entity)
    }

    override fun saveData() {
        processor.insertDataToDataBasePreparedStatement(entityClass, dataForInsert, conn)
        dataForInsert.clear()
    }
}
