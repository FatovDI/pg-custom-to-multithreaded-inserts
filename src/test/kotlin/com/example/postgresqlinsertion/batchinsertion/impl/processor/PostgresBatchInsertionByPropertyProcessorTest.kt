package com.example.postgresqlinsertion.batchinsertion.impl.processor

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.logic.entity.BaseEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.postgresql.util.PSQLException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import java.io.DataOutputStream
import java.io.File
import java.io.FileReader
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.time.LocalDate
import javax.persistence.EntityManager
import javax.sql.DataSource
import kotlin.reflect.KMutableProperty1


@SpringBootTest(classes = [PostgresBatchInsertionByPropertyProcessor::class])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(locations = ["classpath:application-test.properties"])
@EnableAutoConfiguration
@Suppress("UNCHECKED_CAST")
internal class PostgresBatchInsertionByPropertyProcessorTest {

    @Autowired
    lateinit var dataSource: DataSource

    @Autowired
    lateinit var em: EntityManager

    @Autowired
    lateinit var processor: BatchInsertionByPropertyProcessor

    lateinit var conn: Connection

    private val delimiter = "|"
    private val nullValue = "NULL"

    @BeforeEach
    fun setUp() {
        conn = dataSource.connection
    }


    @AfterEach
    fun tearDown() {
        conn.close()
    }

    @ParameterizedTest
    @MethodSource("getTestData")
    fun `update saved data`(params: Pair<String, String>) {
        val paymentPurpose = params.first
        val prop10 = params.second + "7"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop10 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )
        val dataForInsert = mutableListOf<String>()
        dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        processor.insertDataToDataBaseMultiRow(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )
        val savedDoc =
            em.createNativeQuery("select id, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        val pdId = savedDoc.first()[0].toString().toLong()
        assertThat(pdId).isNotNull
        assertThat(savedDoc.first()[1]).isNull()

        val dataForUpdate = mutableListOf<String>()
        val dataUpdate = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to accountId,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to paymentPurpose,
            PaymentDocumentEntity::prop10 to prop10,
        )
        dataForUpdate.add(processor.getStringForUpdate(data = dataUpdate, id = pdId, nullValue = nullValue))
        processor.updateDataToDataBase(
            clazz = PaymentDocumentEntity::class,
            columns = dataUpdate.keys,
            data = dataForUpdate,
            conn = conn
        )

        val updatedDoc =
            em.createNativeQuery("select account_id, prop_15, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(updatedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(updatedDoc.first()[1]).isEqualTo("END")
        assertThat(updatedDoc.first()[2]).isEqualTo(params.first)
    }

    @ParameterizedTest
    @MethodSource("getTestData")
    fun `update saved data prepared statement`(params: Pair<String, String>) {
        val paymentPurpose = params.first
        val prop10 = params.second + "7_ps"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop10 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )
        val dataForInsert = mutableListOf<String>()
        dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        processor.insertDataToDataBaseMultiRow(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )
        val savedDoc =
            em.createNativeQuery("select id, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        val pdId = savedDoc.first()[0].toString().toLong()
        assertThat(pdId).isNotNull
        assertThat(savedDoc.first()[1]).isNull()

        val dataForUpdate = mutableListOf<Collection<Any?>>()
        val dataUpdate = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::account to accountId,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to paymentPurpose,
            PaymentDocumentEntity::prop10 to prop10,
        )
        dataForUpdate.add(dataUpdate.values + pdId)
        processor.updateDataToDataBasePreparedStatement(
            clazz = PaymentDocumentEntity::class,
            columns = dataUpdate.keys,
            data = dataForUpdate,
            conditionParams = listOf("id"),
            conn = conn
        )

        val updatedDoc =
            em.createNativeQuery("select account_id, prop_15, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(updatedDoc.first()[0]).isEqualTo(accountId)
        assertThat(updatedDoc.first()[1]).isEqualTo("END")
        assertThat(updatedDoc.first()[2]).isEqualTo(params.first)
    }

    @Test
    fun `update several data via insert method with prepared statement`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val paymentPurpose = "Updated purpose"
        val prop15 = "END_PROP_PS_upd"
        val orderDate = "2022-01-02"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::cur to cur,
            PaymentDocumentEntity::orderDate to orderDate,
        )
        val dataForInsert = mutableListOf<String>()
        val testData = getTestData()

        testData.forEach {
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        }
        processor.insertDataToDataBaseMultiRow(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select id, payment_purpose, prop_15, prop_10, cur  from payment_document where order_date = '$orderDate' and cur = '$cur'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][1]).isEqualTo(pair.first)
            assertThat(savedDoc[index][2]).isNull()
            assertThat(savedDoc[index][3]).isEqualTo(pair.second)
            assertThat(savedDoc[index][4].toString()).isEqualTo(cur)
        }

        val pdIds = savedDoc.map { it.first().toString().toLong() }
        val dataForUpdate = mutableListOf<Collection<Any?>>()
        val dataUpdate = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::prop15 to prop15,
            PaymentDocumentEntity::paymentPurpose to paymentPurpose,
        )
        testData.forEachIndexed { idx, _ ->
            dataForUpdate.add(dataUpdate.values + pdIds[idx])
        }

        processor.updateDataToDataBasePreparedStatement(
            clazz = PaymentDocumentEntity::class,
            columns = dataUpdate.keys,
            data = dataForUpdate,
            conditionParams = listOf("id"),
            conn = conn
        )

        val updatedDoc =
            em.createNativeQuery("select id, payment_purpose, prop_15, prop_10, cur  from payment_document where order_date = '$orderDate' and cur = '$cur' order by prop_10").resultList as List<Array<Any>>
        assertThat(updatedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(updatedDoc[index][1]).isEqualTo(paymentPurpose)
            assertThat(updatedDoc[index][2]).isEqualTo(prop15)
            assertThat(updatedDoc[index][3]).isEqualTo(pair.second)
            assertThat(updatedDoc[index][4].toString()).isEqualTo(cur)
        }
    }

    @Test
    fun `save several data via insert method`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::cur to cur,
            PaymentDocumentEntity::orderDate to "2022-01-01",
        )
        val testData = getTestData()
        val dataForInsert = mutableListOf<String>()

        testData.forEach {
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        }
        processor.insertDataToDataBaseMultiRow(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, cur  from payment_document where order_date = '2022-01-01' and cur = '$cur'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo("END")
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3].toString()).isEqualTo(cur)
        }
    }

    @Test
    fun `save data with null value via insert method`() {
        val prop10 = "777_null"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to null,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )
        val dataForInsert = mutableListOf<String>()

        dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        processor.insertDataToDataBaseMultiRow(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, account_id  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isNull()
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
        assertThat(savedDoc.first()[3]).isNull()
    }

    @Test
    fun `save data with incorrect value via insert method`() {
        val prop10 = "777_incor"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to "1",
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )
        val dataForInsert = mutableListOf<String>()

        dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        assertThatThrownBy {
            processor.insertDataToDataBaseMultiRow(
                clazz = PaymentDocumentEntity::class,
                columns = data.keys,
                data = dataForInsert,
                conn = conn
            )
        }.isInstanceOf(PSQLException::class.java)
    }


    @Test
    fun `save several data via basic insert method`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::cur to cur,
            PaymentDocumentEntity::orderDate to "2022-01-03",
        )
        val testData = getTestData()
        val dataForInsert = mutableListOf<String>()

        testData.forEach {
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        }
        processor.insertDataToDataBase(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, cur  from payment_document where order_date = '2022-01-01' and cur = '$cur'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo("END")
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3].toString()).isEqualTo(cur)
        }
    }

    @Test
    fun `save data with null value via basic insert method`() {
        val prop10 = "777_null_b"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to null,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )
        val dataForInsert = mutableListOf<String>()

        dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        processor.insertDataToDataBase(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, account_id  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isNull()
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
        assertThat(savedDoc.first()[3]).isNull()
    }

    @Test
    fun `save data with incorrect value via basic insert method`() {
        val prop10 = "777_inc_b"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to "1",
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )
        val dataForInsert = mutableListOf<String>()

        dataForInsert.add(processor.getStringForInsert(data = data, nullValue = nullValue))
        assertThatThrownBy {
            processor.insertDataToDataBase(
                clazz = PaymentDocumentEntity::class,
                columns = data.keys,
                data = dataForInsert,
                conn = conn
            )
        }.isInstanceOf(PSQLException::class.java)
    }

    @Test
    fun `save several entity data via insert with prepared statement method`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val dataForInsert = mutableListOf<List<Any?>>()
        val testData = getTestData()
        val prop15 = "NEW_PROP_PS"

        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>()

        testData.forEach {
            data[PaymentDocumentEntity::prop15] = prop15
            data[PaymentDocumentEntity::cur] = cur
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            dataForInsert.add(data.values.toList())
        }
        processor.insertDataToDataBasePreparedStatement(clazz = PaymentDocumentEntity::class, data.keys, data = dataForInsert, conn = conn)

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, cur  from payment_document where prop_15 = '$prop15' and cur = '$cur'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo(prop15)
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3].toString()).isEqualTo(cur)
        }
    }

    @Test
    fun `save all entity data via insert with prepared statement method`() {
        val dataForInsert = mutableListOf<List<Any?>>()
        val prop10 = "7171_PR_PS"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>()
        data[PaymentDocumentEntity::account] = accountId.toLong()
        data[PaymentDocumentEntity::expense] = false
        data[PaymentDocumentEntity::amount] = BigDecimal("10.11")
        data[PaymentDocumentEntity::cur] = "RUB"
        data[PaymentDocumentEntity::orderDate] = LocalDate.parse("2023-01-01")
        data[PaymentDocumentEntity::orderNumber] = "123"
        data[PaymentDocumentEntity::prop20] = "1345"
        data[PaymentDocumentEntity::prop15] = "END"
        data[PaymentDocumentEntity::paymentPurpose] = null
        data[PaymentDocumentEntity::prop10] = prop10
        dataForInsert.add(data.values.toList())

        processor.insertDataToDataBasePreparedStatement(clazz = PaymentDocumentEntity::class, columns = data.keys, data = dataForInsert, conn = conn)

        val savedDoc = em.createNativeQuery(
            """
                select
                    account_id,
                    expense,
                    cur,
                    amount,
                    order_date,
                    order_number,
                    prop_20,
                    prop_15,
                    payment_purpose,
                    prop_10
                from payment_document where prop_10 = '$prop10'
            """.trimIndent()
        ).resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(savedDoc.first()[1]).isEqualTo(false)
        assertThat(savedDoc.first()[2]).isEqualTo("RUB")
        assertThat(savedDoc.first()[3]).isEqualTo(BigDecimal("10.11"))
        assertThat(savedDoc.first()[4]).isEqualTo(Date.valueOf("2023-01-01"))
        assertThat(savedDoc.first()[5]).isEqualTo("123")
        assertThat(savedDoc.first()[6]).isEqualTo("1345")
        assertThat(savedDoc.first()[7]).isEqualTo("END")
        assertThat(savedDoc.first()[8]).isNull()
        assertThat(savedDoc.first()[9]).isEqualTo(prop10)
    }

    @Test
    fun `save several entity data via insert with prepared statement and unnest method`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val dataForInsert = mutableListOf<List<*>>()
        val testData = getTestData()
        val prop15 = "NEW_PR_PS_UNN"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>()

        testData.forEach {
            data[PaymentDocumentEntity::prop15] = prop15
            data[PaymentDocumentEntity::cur] = cur
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            dataForInsert.add(data.values.toList())
        }
        val pgTypes = processor.getPgTypes(clazz = PaymentDocumentEntity::class, columns = data.keys, conn = conn)
        processor.insertDataToDataBasePreparedStatementAndUnnest(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            pgTypes = pgTypes,
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, cur  from payment_document where prop_15 = '$prop15' and cur = '$cur'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo(prop15)
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3].toString()).isEqualTo(cur)
        }
    }

    @Test
    fun `save all entity data via insert with prepared statement and unnest method`() {
        val dataForInsert = mutableListOf<List<Any?>>()
        val prop10 = "7_PR_PS_UN"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>()
        data[PaymentDocumentEntity::account] = accountId.toLong()
        data[PaymentDocumentEntity::expense] = false
        data[PaymentDocumentEntity::amount] = BigDecimal("10.11")
        data[PaymentDocumentEntity::cur] = "RUB"
        data[PaymentDocumentEntity::orderDate] = LocalDate.parse("2023-01-01")
        data[PaymentDocumentEntity::orderNumber] = "123"
        data[PaymentDocumentEntity::prop20] = "1345"
        data[PaymentDocumentEntity::prop15] = "END"
        data[PaymentDocumentEntity::paymentPurpose] = null
        data[PaymentDocumentEntity::prop10] = prop10
        dataForInsert.add(data.values.toList())
        val pgTypes = processor.getPgTypes(clazz = PaymentDocumentEntity::class, columns = data.keys, conn = conn)

        processor.insertDataToDataBasePreparedStatementAndUnnest(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            data = dataForInsert,
            pgTypes = pgTypes,
            conn = conn
        )

        val savedDoc = em.createNativeQuery(
            """
                select
                    account_id,
                    expense,
                    cur,
                    amount,
                    order_date,
                    order_number,
                    prop_20,
                    prop_15,
                    payment_purpose,
                    prop_10
                from payment_document where prop_10 = '$prop10'
            """.trimIndent()
        ).resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(savedDoc.first()[1]).isEqualTo(false)
        assertThat(savedDoc.first()[2]).isEqualTo("RUB")
        assertThat(savedDoc.first()[3]).isEqualTo(BigDecimal("10.11"))
        assertThat(savedDoc.first()[4]).isEqualTo(Date.valueOf("2023-01-01"))
        assertThat(savedDoc.first()[5]).isEqualTo("123")
        assertThat(savedDoc.first()[6]).isEqualTo("1345")
        assertThat(savedDoc.first()[7]).isEqualTo("END")
        assertThat(savedDoc.first()[8]).isNull()
        assertThat(savedDoc.first()[9]).isEqualTo(prop10)
    }

    @Test
    fun `save incorrect data via copy method`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val prop10 = "666"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to "1",
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.addDataForCreate(data = data, writer = writer, delimiter = delimiter, nullValue = nullValue)
        writer.close()
        assertThatThrownBy {
            processor.saveToDataBaseByCopyMethod(
                clazz = PaymentDocumentEntity::class,
                columns = data.keys,
                delimiter = delimiter,
                nullValue = nullValue,
                from = FileReader(file),
                conn = conn
            )
        }.isInstanceOf(PSQLException::class.java)
    }

    @Test
    fun `save null data via copy method`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val prop10 = "656"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to null,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.addDataForCreate(data = data, writer = writer, delimiter = delimiter, nullValue = nullValue)
        writer.close()
        processor.saveToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            delimiter = delimiter,
            nullValue = nullValue,
            from = FileReader(file),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, account_id  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isNull()
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
        assertThat(savedDoc.first()[3]).isNull()
    }

    @Test
    fun `save all data via copy method`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val prop10 = "717"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::account to accountId,
            PaymentDocumentEntity::amount to "10.11",
            PaymentDocumentEntity::expense to "true",
            PaymentDocumentEntity::cur to "RUB",
            PaymentDocumentEntity::orderDate to "2023-01-01",
            PaymentDocumentEntity::orderNumber to "123",
            PaymentDocumentEntity::prop20 to "1345",
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.addDataForCreate(data = data, writer = writer, delimiter = delimiter, nullValue = nullValue)
        writer.close()
        processor.saveToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            delimiter = delimiter,
            nullValue = nullValue,
            from = FileReader(file),
            conn = conn
        )

        val savedDoc = em.createNativeQuery(
            """
                select
                    account_id,
                    amount,
                    expense,
                    cur,
                    order_date,
                    order_number,
                    prop_20,
                    prop_15,
                    payment_purpose,
                    prop_10
                from payment_document where prop_10 = '$prop10'
            """.trimIndent()
        ).resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(savedDoc.first()[1]).isEqualTo(BigDecimal("10.11"))
        assertThat(savedDoc.first()[2]).isEqualTo(true)
        assertThat(savedDoc.first()[3]).isEqualTo("RUB")
        assertThat(savedDoc.first()[4]).isEqualTo(Date.valueOf("2023-01-01"))
        assertThat(savedDoc.first()[5]).isEqualTo("123")
        assertThat(savedDoc.first()[6]).isEqualTo("1345")
        assertThat(savedDoc.first()[7]).isEqualTo("END")
        assertThat(savedDoc.first()[8]).isNull()
        assertThat(savedDoc.first()[9]).isEqualTo(prop10)
    }

    @Test
    fun `save several data via copy method`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val prop20 = "7777877"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::prop20 to "7777877",
        )
        val testData = getTestData()

        testData.forEach {
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            processor.addDataForCreate(data = data, writer = writer, delimiter = delimiter, nullValue = nullValue)
        }
        writer.close()
        processor.saveToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            delimiter = delimiter,
            nullValue = nullValue,
            from = FileReader(file),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, prop_20  from payment_document where prop_20 = '$prop20'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo("END")
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3]).isEqualTo(prop20)
        }
    }

    @Test
    fun `save data via copy method with comma delimiter`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to "бла бла , бла блабла",
            PaymentDocumentEntity::prop10 to "111",
        )

        processor.addDataForCreate(data = data, writer = writer, delimiter = ",", nullValue = nullValue)
        writer.close()
        processor.saveToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class, columns = data.keys,
            delimiter = ",",
            nullValue = nullValue,
            from = FileReader(file),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10  from payment_document where prop_10 = '111'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isEqualTo("бла бла , бла блабла")
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo("111")
    }

    @ParameterizedTest
    @MethodSource("getTestData")
    fun `save via copy method with all special symbols`(params: Pair<String, String>) {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val paymentPurpose = params.first
        val prop10 = params.second + "6"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, String?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to paymentPurpose,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.addDataForCreate(data = data, writer = writer, delimiter = delimiter, nullValue = nullValue)
        writer.close()
        processor.saveToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class, columns = data.keys,
            delimiter = delimiter,
            nullValue = nullValue,
            from = FileReader(file),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isEqualTo(paymentPurpose)
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
    }

    @Test
    fun `save incorrect data via copy method by binary`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val prop10 = "666_b"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::account to 1L,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.startSaveBinaryDataForCopyMethod(writer)
        processor.addDataForCreateWithBinary(data = data, outputStream = writer)
        processor.endSaveBinaryDataForCopyMethod(writer)

        assertThatThrownBy {
            processor.saveBinaryToDataBaseByCopyMethod(
                clazz = PaymentDocumentEntity::class,
                columns = data.keys,
                from = file.inputStream(),
                conn = conn
            )
        }.isInstanceOf(PSQLException::class.java)
    }

    @Test
    fun `save null data via copy method by binary`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val prop10 = "656_b"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::account to null,
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.startSaveBinaryDataForCopyMethod(writer)
        processor.addDataForCreateWithBinary(data = data, outputStream = writer)
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            from = file.inputStream(),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, account_id  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isNull()
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
        assertThat(savedDoc.first()[3]).isNull()
    }

    @Test
    fun `save all data via copy method by binary`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val prop10 = "717_b"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString().toLong()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::account to accountId,
            PaymentDocumentEntity::amount to BigDecimal("10.11"),
            PaymentDocumentEntity::expense to true,
            PaymentDocumentEntity::cur to "RUB",
            PaymentDocumentEntity::orderDate to LocalDate.parse("2023-01-01"),
            PaymentDocumentEntity::orderNumber to "123",
            PaymentDocumentEntity::prop20 to "1345",
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to null,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.startSaveBinaryDataForCopyMethod(writer)
        processor.addDataForCreateWithBinary(data = data, outputStream = writer)
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            from = file.inputStream(),
            conn = conn
        )

        val savedDoc = em.createNativeQuery(
            """
                select
                    account_id,
                    amount,
                    expense,
                    cur,
                    order_date,
                    order_number,
                    prop_20,
                    prop_15,
                    payment_purpose,
                    prop_10
                from payment_document where prop_10 = '$prop10'
            """.trimIndent()
        ).resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0].toString().toLong()).isEqualTo(accountId)
        assertThat(savedDoc.first()[1]).isEqualTo(BigDecimal("10.11"))
        assertThat(savedDoc.first()[2]).isEqualTo(true)
        assertThat(savedDoc.first()[3]).isEqualTo("RUB")
        assertThat(savedDoc.first()[4]).isEqualTo(Date.valueOf("2023-01-01"))
        assertThat(savedDoc.first()[5]).isEqualTo("123")
        assertThat(savedDoc.first()[6]).isEqualTo("1345")
        assertThat(savedDoc.first()[7]).isEqualTo("END")
        assertThat(savedDoc.first()[8]).isNull()
        assertThat(savedDoc.first()[9]).isEqualTo(prop10)
    }

    @Test
    fun `save several data via copy method by binary`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val prop20 = "7777877_b"
        val testData = getTestData()
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::prop20 to prop20,
        )

        processor.startSaveBinaryDataForCopyMethod(writer)
        testData.forEach {
            data[PaymentDocumentEntity::paymentPurpose] = it.first
            data[PaymentDocumentEntity::prop10] = it.second
            processor.addDataForCreateWithBinary(data = data, outputStream = writer)
        }
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class,
            columns = data.keys,
            from = file.inputStream(),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, prop_20  from payment_document where prop_20 = '$prop20'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo("END")
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3]).isEqualTo(prop20)
        }
    }

    @ParameterizedTest
    @MethodSource("getTestData")
    fun `save via copy method by binary with all special symbols`(params: Pair<String, String>) {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val paymentPurpose = params.first
        val prop10 = params.second + "6_b"
        val data = mutableMapOf<KMutableProperty1<out BaseEntity, *>, Any?>(
            PaymentDocumentEntity::prop15 to "END",
            PaymentDocumentEntity::paymentPurpose to paymentPurpose,
            PaymentDocumentEntity::prop10 to prop10,
        )

        processor.startSaveBinaryDataForCopyMethod(writer)
        processor.addDataForCreateWithBinary(data = data, outputStream = writer)
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(
            clazz = PaymentDocumentEntity::class, columns = data.keys,
            from = file.inputStream(),
            conn = conn
        )

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isEqualTo(paymentPurpose)
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
    }

    companion object {
        @JvmStatic
        fun getTestData(): List<Pair<String, String>> {
            return listOf(
                Pair("бла бла", "222"),
                Pair("бла бла | бла блабла", "333"),
                Pair("б`л~а !б@л#а№;ж\$s%u ^s p &l? z* (d)- _s+= /W\\|{we}[ct]a,r<cs.>w's", "444"),
                Pair("бла\b бла \n бла \r бла \tбла бла", "555"),
                Pair("select id from account limit 1", "666"),
                Pair("'select id from account limit 1'", "777"),
                Pair("'select id from account limit 1", "888"),
                Pair("--select id from account limit 1", "999"),
            )
        }

    }

}
