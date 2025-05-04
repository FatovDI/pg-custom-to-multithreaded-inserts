package com.example.postgresqlinsertion.batchinsertion.api.processor

import com.example.postgresqlinsertion.logic.entity.BaseEntity
import java.io.BufferedWriter
import java.io.DataOutputStream
import java.io.InputStream
import java.io.Reader
import java.sql.Connection
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1

/**
 * For save or update Map<out KProperty1<out BaseEntity, *>, String?>
 */
interface BatchInsertionByPropertyProcessor {

    /**
     * add data for create by map of property
     * @param data - map of property and value Example: PaymentDocumentEntity::operationDt
     * @param writer - BufferedWriter for write entity to file
     * @param delimiter - delimiter for separate data
     * @param nullValue - string to define null value
     */
    fun addDataForCreate(
        data: Map<out KProperty1<out BaseEntity, *>, Any?>,
        writer: BufferedWriter,
        delimiter: String,
        nullValue: String
    )

    /**
     * start save binary data for copy method
     * @param outputStream - data output stream with data for save
     */
    fun startSaveBinaryDataForCopyMethod(outputStream: DataOutputStream)

    /**
     * end save binary data for copy method
     * @param outputStream - data output stream with data for save
     */
    fun endSaveBinaryDataForCopyMethod(outputStream: DataOutputStream)

    /**
     * add data for create by map of property
     * @param data - map of property and value as string. Example: PaymentDocumentEntity::operationDt
     * @param outputStream - output stream for write data
     */
    fun addDataForCreateWithBinary(
        data: Map<out KProperty1<out BaseEntity, *>, Any?>,
        outputStream: DataOutputStream
    )

    /**
     * get string for update by map of property
     * @param data - map of property and value. Example: PaymentDocumentEntity::operationDt
     * @param id - id entity
     * @param nullValue - string to define null value
     * @return String - string for update
     */
    fun getStringForUpdate(data: Map<out KProperty1<out BaseEntity, *>, Any?>, id: Long, nullValue: String): String

    /**
     * get string for update by map of property
     * @param data - map of property and value. Example: PaymentDocumentEntity::operationDt
     * @param nullValue - string to define null value
     * @return String - string for update
     */
    fun getStringForInsert(data: Map<out KProperty1<out BaseEntity, *>, Any?>, nullValue: String): String

    /**
     * save data via file by property
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param delimiter - delimiter for separate data
     * @param nullValue - string to define null value
     * @param from - data for save
     * @param conn - DB connection
     */
    fun saveToDataBaseByCopyMethod(
        clazz: KClass<out BaseEntity>,
        columns: Set<KProperty1<out BaseEntity, *>>,
        delimiter: String,
        nullValue: String,
        from: Reader,
        conn: Connection
    )

    /**
     * save data via file by property
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param from - input stream with data for save
     * @param conn - DB connection
     */
    fun saveBinaryToDataBaseByCopyMethod(
        clazz: KClass<out BaseEntity>,
        columns: Set<KProperty1<out BaseEntity, *>>,
        from: InputStream,
        conn: Connection
    )

    /**
     * save list data with basic insert method by property
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param data - list of string
     * @param conn - DB connection
     */
    fun insertDataToDataBase(
        clazz: KClass<out BaseEntity>, columns: Set<KProperty1<out BaseEntity, *>>, data: List<String>, conn: Connection
    )

    /**
     * save list data with insert method by property multi row
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param data - list of string
     * @param conn - DB connection
     */
    fun insertDataToDataBaseMultiRow(
        clazz: KClass<out BaseEntity>, columns: Set<KProperty1<out BaseEntity, *>>, data: List<String>, conn: Connection
    )

    /**
     * save list data with insert method and prepared statement
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param data - list of string by columns
     * @param conn - DB connection
     */
    fun insertDataToDataBasePreparedStatement(
        clazz: KClass<out BaseEntity>,
        columns: Set<KProperty1<out BaseEntity, *>>,
        data: List<List<Any?>>,
        conn: Connection
    )

    /**
     * save list data with insert method and prepared statement basic
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param data - list of string by columns
     * @param conn - DB connection
     */
    fun insertDataToDataBasePreparedStatementBasic(
        clazz: KClass<out BaseEntity>,
        columns: Set<KProperty1<out BaseEntity, *>>,
        data: List<List<Any?>>,
        conn: Connection
    )

    /**
     * save list data with insert method and prepared statement and select data by unnest
     * @param clazz - entity class
     * @param columns - columns for get pg Types
     * @param data - list of string by columns
     * @param pgTypes - list of pg types
     * @param conn - DB connection
     */
    fun insertDataToDataBasePreparedStatementAndUnnest(
        clazz: KClass<out BaseEntity>,
        columns: Set<KProperty1<out BaseEntity, *>>,
        data: List<List<*>>,
        pgTypes: List<String>,
        conn: Connection
    )

    /**
     * get pg types by columns
     * @param clazz - entity class
     * @param columns - columns for get pg Types
     * @param conn - DB connection
     * @return List<String> - list with pg type name
     */
    fun getPgTypes(clazz: KClass<out BaseEntity>, columns: Set<KProperty1<out BaseEntity, *>>, conn: Connection): List<String>

    /**
     * save list data with update method by set of property
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param data - list of string
     * @param conn - DB connection
     */
    fun updateDataToDataBase(
        clazz: KClass<out BaseEntity>, columns: Set<KProperty1<out BaseEntity, *>>, data: List<String>, conn: Connection
    )

    /**
     * save list data with update method by set of property and prepared statement
     * @param clazz - entity class
     * @param columns - set of entity property
     * @param data - list with batched data
     * @param conditionParams - list with names of parameter
     * @param conn - DB connection
     * @return count updated rows
     */
    fun updateDataToDataBasePreparedStatement(
        clazz: KClass<out BaseEntity>,
        columns: Set<KProperty1<out BaseEntity, *>>,
        data: Collection<Collection<Any?>>,
        conditionParams: Collection<String>,
        conn: Connection
    ): Int

}
