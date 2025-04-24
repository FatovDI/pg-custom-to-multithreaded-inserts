package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.batchinsertion.exception.BatchInsertionException
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.memberProperties

open class UpdateByPropertySaver<E: BaseEntity>(
    private val processor: BatchInsertionByPropertyProcessor,
    private val entityClass: KClass<E>,
    conn: Connection,
    batchSize: Int
) : AbstractBatchInsertionByPropertySaver<E>(conn, batchSize) {

    private val nullValue = "NULL"
    private val dataForUpdate = mutableListOf<String>()
    private val idProp = entityClass.memberProperties.find { it.name == "id" }
        ?: throw BatchInsertionException("Id should be defined in entity for update")

    override fun addDataForSave(data: Map<out KProperty1<E, *>, Any?>) {
        val id = data[idProp]?.toString()?.toLongOrNull()
            ?: throw BatchInsertionException("Id should be not null for update")

        dataForUpdate.add(processor.getStringForUpdate(data, id, nullValue))
        super.addDataForSave(data)
    }

    override fun saveData() {
        super.saveData()
        processor.updateDataToDataBase(entityClass, columns!!, dataForUpdate, conn)
        dataForUpdate.clear()
    }
}
