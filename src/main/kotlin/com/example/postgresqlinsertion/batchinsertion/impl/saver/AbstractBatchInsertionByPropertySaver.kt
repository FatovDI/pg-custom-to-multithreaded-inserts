package com.example.postgresqlinsertion.batchinsertion.impl.saver

import com.example.postgresqlinsertion.batchinsertion.api.saver.BatchInsertionByPropertySaver
import com.example.postgresqlinsertion.batchinsertion.exception.BatchInsertionException
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.sql.Connection
import kotlin.reflect.KProperty1

abstract class AbstractBatchInsertionByPropertySaver<E : BaseEntity>(
    conn: Connection,
    private val batchSize: Int
) : AbstractBatchInsertionSaver(conn), BatchInsertionByPropertySaver<E> {

    private var counter = 0
    var columns: Set<KProperty1<E, *>>? = null

    override fun addDataForSave(data: Map<out KProperty1<E, *>, Any?>) {
        columns?.let { it.takeIf { data.keys == it } ?: throw BatchInsertionException("Keys should be equal in one saver") }
        columns ?: let { columns = data.keys }

        counter++
        if (counter % batchSize == 0) {
            log.info("save batch insertion $batchSize")
            saveData()
        }
    }

    override fun saveData() {
        columns ?: throw BatchInsertionException("Columns should be defined in the saver")
    }

    override fun commit() {
        if (counter % batchSize != 0) {
            saveData()
        }
        log.info("start commit $counter data")
        counter = 0
        super.commit()
    }
}