package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByPropertySaver
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

open class InsertByPropertyPreparedStatementSaver<E: BaseEntity>(
    private val processor: BatchInsertionByPropertyProcessor,
    private val entityClass: KClass<E>,
    conn: Connection,
) : AbstractBatchInsertionSaver(conn), BatchInsertionByPropertySaver<E> {

    private val dataForInsert = mutableListOf<List<Any?>>()

    override fun addDataForSave(data: Map<out KProperty1<E, *>, Any?>) {
        dataForInsert.add(data.values.toList())
    }

    override fun saveData(columns: Set<KProperty1<E, *>>) {
        processor.insertDataToDataBasePreparedStatement(entityClass, columns, dataForInsert, conn)
        dataForInsert.clear()
    }
}
