package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.io.BufferedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

open class CopyBinaryByPropertySaver<E: BaseEntity>(
    private val processor: BatchInsertionByPropertyProcessor,
    private val entityClass: KClass<E>,
    conn: Connection,
    batchSize: Int
) : AbstractBatchInsertionByPropertySaver<E>(conn, batchSize) {

    private var byteArrayOs = ByteArrayOutputStream()
    private var writer = DataOutputStream(BufferedOutputStream(byteArrayOs))

    init {
        processor.startSaveBinaryDataForCopyMethod(writer)
    }

    override fun addDataForSave(data: Map<out KProperty1<E, *>, Any?>) {
        processor.addDataForCreateWithBinary(data, writer)
        super.addDataForSave(data)
    }

    override fun saveData() {
        super.saveData()
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(
            clazz = entityClass,
            columns = columns!!,
            from = byteArrayOs.toByteArray().inputStream(),
            conn = conn
        )
        byteArrayOs = ByteArrayOutputStream()
        writer = DataOutputStream(BufferedOutputStream(byteArrayOs))
        processor.startSaveBinaryDataForCopyMethod(writer)
    }
}
