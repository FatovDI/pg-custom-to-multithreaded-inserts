package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.getDataFromEntity
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import kotlin.reflect.KClass

open class UpdateByEntityPreparedStatementSaver<E: BaseEntity>(
    private val processor: BatchInsertionByEntityProcessor,
    private val entityClass: KClass<E>,
    conn: Connection,
    batchSize: Int
) : AbstractBatchInsertionByEntitySaver<E>(conn, batchSize) {

    private val dataForUpdate = mutableListOf<List<Any?>>()
    private val conditions = listOf("id")

    override fun addDataForSave(entity: E) {
        dataForUpdate.add(getDataFromEntity(entity) + entity.id)
        super.addDataForSave(entity)
    }

    override fun saveData() {
        processor.updateDataToDataBasePreparedStatement(entityClass, dataForUpdate, conditions, conn)
        dataForUpdate.clear()
    }
}
