package com.example.postgresqlinsertion.batchinsertion.impl.processor

import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByEntityProcessor
import com.example.postgresqlinsertion.batchinsertion.getDataFromEntity
import com.example.postgresqlinsertion.logic.entity.AccountEntity
import com.example.postgresqlinsertion.logic.entity.CurrencyEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
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


@SpringBootTest(classes = [PostgresBatchInsertionByEntityProcessor::class])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@TestPropertySource(locations = ["classpath:application-test.properties"])
@EnableAutoConfiguration
@Suppress("UNCHECKED_CAST")
internal class PostgresBatchInsertionByEntityProcessorTest {

    @Autowired
    lateinit var dataSource: DataSource

    @Autowired
    lateinit var em: EntityManager

    @Autowired
    lateinit var processor: BatchInsertionByEntityProcessor

    lateinit var conn: Connection

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
    fun `update saved data via entity`(params: Pair<String, String>) {
        val paymentPurpose = params.first
        val prop10 = params.second + "8"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = PaymentDocumentEntity(
            prop15 = "END",
            paymentPurpose = null,
            prop10 = prop10,
        )
        val dataForInsert = mutableListOf<String>()
        dataForInsert.add(processor.getStringForInsert(data))
        processor.insertDataToDataBaseMultiRow(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)
        val savedDoc =
            em.createNativeQuery("select id, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        val pdId = savedDoc.first()[0].toString().toLong()
        assertThat(pdId).isNotNull
        assertThat(savedDoc.first()[1]).isNull()

        val dataForUpdate = mutableListOf<String>()
        val dataUpdate = PaymentDocumentEntity(
            account = AccountEntity().apply { id = accountId.toLong() },
            prop15 = "END",
            paymentPurpose = paymentPurpose,
            prop10 = prop10,
        ).apply { id = pdId }
        dataForUpdate.add(processor.getStringForUpdate(dataUpdate))
        processor.updateDataToDataBase(clazz = PaymentDocumentEntity::class, data = dataForUpdate, conn = conn)

        val updatedDoc =
            em.createNativeQuery("select account_id, prop_15, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(updatedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(updatedDoc.first()[1]).isEqualTo("END")
        assertThat(updatedDoc.first()[2]).isEqualTo(params.first)
    }

    @ParameterizedTest
    @MethodSource("getTestData")
    fun `insert data via entity with basic method`(params: Pair<String, String>) {
        val paymentPurpose = params.first
        val prop10 = params.second + "8_b"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = PaymentDocumentEntity(
            account = AccountEntity().apply { id = accountId.toLong() },
            prop15 = "END",
            paymentPurpose = paymentPurpose,
            prop10 = prop10,
        )
        val dataForInsert = mutableListOf<String>()
        dataForInsert.add(processor.getStringForInsert(data))
        processor.insertDataToDataBase(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)
        val savedDoc =
            em.createNativeQuery("select account_id, prop_15, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(params.first)
    }

    @ParameterizedTest
    @MethodSource("getTestData")
    fun `update saved data via entity prepared statement`(params: Pair<String, String>) {
        val paymentPurpose = params.first
        val prop10 = params.second + "8_ps"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = PaymentDocumentEntity(
            prop15 = "END",
            paymentPurpose = null,
            prop10 = prop10,
        )
        val dataForInsert = mutableListOf<List<Any?>>()
        dataForInsert.add(getDataFromEntity(data))
        processor.insertDataToDataBasePreparedStatement(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)
        val savedDoc =
            em.createNativeQuery("select id, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        val pdId = savedDoc.first()[0].toString().toLong()
        assertThat(pdId).isNotNull
        assertThat(savedDoc.first()[1]).isNull()

        val dataForUpdate = mutableListOf<List<Any?>>()
        val dataUpdate = PaymentDocumentEntity(
            account = AccountEntity().apply { id = accountId.toLong() },
            prop15 = "END",
            paymentPurpose = paymentPurpose,
            prop10 = prop10,
        ).apply { id = pdId }
        dataForUpdate.add(getDataFromEntity(dataUpdate) + pdId)
        val conditions = listOf("id")
        processor.updateDataToDataBasePreparedStatement(
            clazz = PaymentDocumentEntity::class,
            data = dataForUpdate,
            conditionParams = conditions,
            conn = conn
        )

        val updatedDoc =
            em.createNativeQuery("select account_id, prop_15, payment_purpose  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(updatedDoc.first()[0].toString()).isEqualTo(accountId)
        assertThat(updatedDoc.first()[1]).isEqualTo("END")
        assertThat(updatedDoc.first()[2]).isEqualTo(params.first)
    }

    @Test
    fun `update several entity data via insert with prepared statement method`() {
        val dataForInsert = mutableListOf<List<Any?>>()
        val paymentPurpose = "Updated purpose"
        val prop15 = "END_PS_upd"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val testData = getTestData()

        testData.forEach {
            val data = PaymentDocumentEntity(
                prop15 = "NEW_PS_upd",
                paymentPurpose = null,
                prop10 = it.second,
            )
            dataForInsert.add(getDataFromEntity(data))
        }
        processor.insertDataToDataBasePreparedStatement(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)

        val savedDoc =
            em.createNativeQuery("select id, payment_purpose, prop_15, prop_10, cur  from payment_document where prop_15 = 'NEW_PS_upd'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        val pdIds = savedDoc.map { it.first().toString().toLong() }
        assertThat(pdIds.size).isEqualTo(testData.size)
        savedDoc.forEach {
            assertThat(it[1]).isNull()
        }

        val dataForUpdate = mutableListOf<List<Any?>>()
        testData.forEachIndexed { idx, d ->
            dataForUpdate.add(getDataFromEntity(
                PaymentDocumentEntity(
                    account = AccountEntity().apply { id = accountId.toLong() },
                    prop15 = prop15,
                    paymentPurpose = paymentPurpose,
                    prop10 = d.second,
                ).apply { id = pdIds[idx] }
            ) + pdIds[idx])
        }
        val conditions = listOf("id")
        processor.updateDataToDataBasePreparedStatement(
            clazz = PaymentDocumentEntity::class,
            data = dataForUpdate,
            conditionParams = conditions,
            conn = conn
        )

        val updatedDoc =
            em.createNativeQuery("select account_id, prop_15, prop_10, payment_purpose  from payment_document where prop_15 = '$prop15'").resultList as List<Array<Any>>

        assertThat(updatedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(updatedDoc[index][0].toString()).isEqualTo(accountId)
            assertThat(updatedDoc[index][1]).isEqualTo(prop15)
            assertThat(updatedDoc[index][2]).isEqualTo(pair.second)
            assertThat(updatedDoc[index][3]).isEqualTo(paymentPurpose)
        }
    }

    @Test
    fun `save several entity data via insert method`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val dataForInsert = mutableListOf<String>()
        val testData = getTestData()

        testData.forEach {
            val data = PaymentDocumentEntity(
                prop15 = "NEW",
                cur = CurrencyEntity(code = cur),
                paymentPurpose = it.first,
                prop10 = it.second,
            )
            dataForInsert.add(processor.getStringForInsert(data))
        }
        processor.insertDataToDataBaseMultiRow(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10, cur  from payment_document where prop_15 = 'NEW' and cur = '$cur'").resultList as List<Array<Any>>
        assertThat(savedDoc.size).isEqualTo(testData.size)
        testData.forEachIndexed { index, pair ->
            assertThat(savedDoc[index][0]).isEqualTo(pair.first)
            assertThat(savedDoc[index][1]).isEqualTo("NEW")
            assertThat(savedDoc[index][2]).isEqualTo(pair.second)
            assertThat(savedDoc[index][3].toString()).isEqualTo(cur)
        }
    }

    @Test
    fun `save several entity data via insert with prepared statement method`() {
        val cur = em.createNativeQuery("select code from currency limit 1").singleResult.toString()
        val dataForInsert = mutableListOf<List<Any?>>()
        val testData = getTestData()
        val prop15 = "NEW_PS"

        testData.forEach {
            val data = PaymentDocumentEntity(
                prop15 = prop15,
                cur = CurrencyEntity(code = cur),
                paymentPurpose = it.first,
                prop10 = it.second,
            )
            dataForInsert.add(getDataFromEntity(data))
        }
        processor.insertDataToDataBasePreparedStatement(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)

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
        val prop10 = "7171_PS"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        dataForInsert.add(
            getDataFromEntity(
                PaymentDocumentEntity(
                    account = AccountEntity().apply { id = accountId.toLong() },
                    expense = false,
                    amount = BigDecimal("10.11"),
                    cur = CurrencyEntity(code = "RUB"),
                    orderDate = LocalDate.parse("2023-01-01"),
                    orderNumber = "123",
                    prop20 = "1345",
                    prop15 = "END",
                    paymentPurpose = null,
                    prop10 = prop10,
                )
            )
        )

        processor.insertDataToDataBasePreparedStatement(clazz = PaymentDocumentEntity::class, data = dataForInsert, conn = conn)

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
        val prop15 = "NEW_PS_UNNEST"
        val pgTypes = processor.getPgTypes(clazz = PaymentDocumentEntity::class, conn = conn)

        testData.forEach {
            val data = PaymentDocumentEntity(
                prop15 = prop15,
                cur = CurrencyEntity(code = cur),
                paymentPurpose = it.first,
                prop10 = it.second,
            )
            dataForInsert.add(getDataFromEntity(data))
        }
        processor.insertDataToDataBasePreparedStatementAndUnnest(
            clazz = PaymentDocumentEntity::class,
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
        val prop10 = "7171_PS_UN"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val pgTypes = processor.getPgTypes(clazz = PaymentDocumentEntity::class, conn = conn)
        dataForInsert.add(
            getDataFromEntity(
                PaymentDocumentEntity(
                    account = AccountEntity().apply { id = accountId.toLong() },
                    expense = false,
                    amount = BigDecimal("10.11"),
                    cur = CurrencyEntity(code = "RUB"),
                    orderDate = LocalDate.parse("2023-01-01"),
                    orderNumber = "123",
                    prop20 = "1345",
                    prop15 = "END",
                    paymentPurpose = null,
                    prop10 = prop10,
                )
            )
        )

        processor.insertDataToDataBasePreparedStatementAndUnnest(
            clazz = PaymentDocumentEntity::class,
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
    fun `save all entity data via copy method`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val prop10 = "7171"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = PaymentDocumentEntity(
            account = AccountEntity().apply { id = accountId.toLong() },
            expense = false,
            amount = BigDecimal("10.11"),
            cur = CurrencyEntity(code = "RUB"),
            orderDate = LocalDate.parse("2023-01-01"),
            orderNumber = "123",
            prop20 = "1345",
            prop15 = "END",
            paymentPurpose = null,
            prop10 = prop10,
        )

        processor.addDataForCreate(data, writer)
        writer.close()
        processor.saveToDataBaseByCopyMethod(clazz = PaymentDocumentEntity::class, from = FileReader(file), conn = conn)

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
    fun `save several entity data via copy method`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val prop20 = "77778778"
        val testData = getTestData()

        testData.forEach {
            val data = PaymentDocumentEntity(
                prop15 = "END",
                prop20 = prop20,
                paymentPurpose = it.first,
                prop10 = it.second,
            )
            processor.addDataForCreate(data, writer)
        }
        writer.close()
        processor.saveToDataBaseByCopyMethod(clazz = PaymentDocumentEntity::class, from = FileReader(file), conn = conn)

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
    fun `save entity via copy method with all special symbols`(params: Pair<String, String>) {
        val file = File(this::class.java.getResource("").file, "/PD_Test.csv")
        val writer = file.bufferedWriter()
        val paymentPurpose = params.first
        val prop10 = params.second + 1
        val data = PaymentDocumentEntity(
            prop15 = "END",
            paymentPurpose = paymentPurpose,
            prop10 = prop10,
        )

        processor.addDataForCreate(data = data, writer = writer)
        writer.close()
        processor.saveToDataBaseByCopyMethod(clazz = PaymentDocumentEntity::class, from = FileReader(file), conn = conn)

        val savedDoc =
            em.createNativeQuery("select payment_purpose, prop_15, prop_10  from payment_document where prop_10 = '$prop10'").resultList as List<Array<Any>>
        assertThat(savedDoc.first()[0]).isEqualTo(paymentPurpose)
        assertThat(savedDoc.first()[1]).isEqualTo("END")
        assertThat(savedDoc.first()[2]).isEqualTo(prop10)
    }

    @Test
    fun `save all entity data via copy method with binary`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val prop10 = "7171_b"
        val accountId = em.createNativeQuery("select id from account limit 1").singleResult.toString()
        val data = PaymentDocumentEntity(
            account = AccountEntity().apply { id = accountId.toLong() },
            expense = false,
            amount = BigDecimal("10.11"),
            cur = CurrencyEntity(code = "RUB"),
            orderDate = LocalDate.parse("2023-01-01"),
            orderNumber = "123",
            prop20 = "1345",
            prop15 = "END",
            paymentPurpose = null,
            prop10 = prop10,
        )

        processor.startSaveBinaryDataForCopyMethod(writer)
        processor.addDataForCreateWithBinary(data, writer)
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(clazz = PaymentDocumentEntity::class, from = file.inputStream(), conn = conn)

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
    fun `save several entity data via copy method with binary`() {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val prop20 = "77778778_b"
        val testData = getTestData()

        processor.startSaveBinaryDataForCopyMethod(writer)

        testData.forEach {
            val data = PaymentDocumentEntity(
                prop15 = "END",
                prop20 = prop20,
                paymentPurpose = it.first,
                prop10 = it.second,
            )
            processor.addDataForCreateWithBinary(data, writer)
        }
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(clazz = PaymentDocumentEntity::class, from = file.inputStream(), conn = conn)

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
    fun `save entity via copy method with all special symbols with binary`(params: Pair<String, String>) {
        val file = File(this::class.java.getResource("").file, "/PD_Test")
        val writer = DataOutputStream(file.outputStream())
        val paymentPurpose = params.first
        val prop10 = params.second + "b"

        processor.startSaveBinaryDataForCopyMethod(writer)

        val data = PaymentDocumentEntity(
            prop15 = "END",
            paymentPurpose = paymentPurpose,
            prop10 = prop10,
        )

        processor.addDataForCreateWithBinary(data, writer)
        processor.endSaveBinaryDataForCopyMethod(writer)
        processor.saveBinaryToDataBaseByCopyMethod(clazz = PaymentDocumentEntity::class, from = file.inputStream(), conn = conn)

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
