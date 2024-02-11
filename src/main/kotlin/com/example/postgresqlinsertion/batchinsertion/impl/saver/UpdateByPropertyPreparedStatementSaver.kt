package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByPropertySaver
import com.example.postgresqlinsertion.batchinsertion.exception.BatchInsertionException
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

open class UpdateByPropertyPreparedStatementSaver<E: BaseEntity>(
    private val processor: BatchInsertionByPropertyProcessor,
    private val entityClass: KClass<E>,
    conn: Connection,
) : AbstractBatchInsertionSaver(conn), BatchInsertionByPropertySaver<E> {

    private val conditions = listOf("id")
    private val dataForUpdate = mutableListOf<Collection<Any?>>()
    private val idProp = entityClass.memberProperties.find { it.name == "id" }
        ?: throw BatchInsertionException("Id should be defined in entity for update")

    override fun addDataForSave(data: Map<out KProperty1<E, *>, Any?>) {
        val id = data[idProp]?.toString()?.toLongOrNull()
            ?: throw BatchInsertionException("Id should be not null for update")

        dataForUpdate.add(data.values + id)
    }

    override fun saveData(columns: Set<KProperty1<E, *>>) {
        processor.updateDataToDataBasePreparedStatement(entityClass, columns, dataForUpdate, conditions, conn)
        dataForUpdate.clear()
    }
}
