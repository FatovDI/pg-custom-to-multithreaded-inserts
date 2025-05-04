package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.batchinsertion.api.SqlHelper
import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByEntityFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByPropertyFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.SaverType
import com.example.postgresqlinsertion.batchinsertion.api.processor.BatchInsertionByPropertyProcessor
import com.example.postgresqlinsertion.batchinsertion.utils.getRandomString
import com.example.postgresqlinsertion.batchinsertion.utils.logger
import com.example.postgresqlinsertion.logic.entity.AccountEntity
import com.example.postgresqlinsertion.logic.entity.CurrencyEntity
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import com.example.postgresqlinsertion.logic.repository.AccountRepository
import com.example.postgresqlinsertion.logic.repository.CurrencyRepository
import com.example.postgresqlinsertion.logic.repository.PaymentDocumentCustomRepository
import org.springframework.stereotype.Service
import java.math.BigDecimal
import java.time.LocalDate
import javax.sql.DataSource
import kotlin.random.Random
import kotlin.reflect.KMutableProperty1


@Service
class PaymentDocumentService(
    private val accountRepo: AccountRepository,
    private val currencyRepo: CurrencyRepository,
    private val sqlHelper: SqlHelper,
    private val pdBatchByEntitySaverFactory: BatchInsertionByEntityFactory<PaymentDocumentEntity>,
    private val pdBatchByPropertySaverFactory: BatchInsertionByPropertyFactory<PaymentDocumentEntity>,
    private val pdCustomRepository: PaymentDocumentCustomRepository,
    private val byPropertyProcessor: BatchInsertionByPropertyProcessor,
    private val dataSource: DataSource,
) {

    private val log by logger()

    fun saveByCopyConcurrent(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_CONCURRENT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopy(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyBinary(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_BINARY).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyAndKProperty(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start collect data for copy saver by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end save data by copy method by property $count")
    }

    fun saveByCopyBinaryAndKProperty(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start collect binary data for copy saver by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY_BINARY).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end save binary data by copy method by property $count")
    }

    fun saveByCopyViaFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyViaBinaryFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.COPY_BINARY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByCopyAnpPropertyViaFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start creation file by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end save file by property $count")

    }

    fun saveByCopyAnpPropertyViaBinaryFile(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start creation binary file by property $count")

        pdBatchByPropertySaverFactory.getSaver(SaverType.COPY_BINARY_VIA_FILE).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end save binary file by property $count")

    }

    fun saveByInsertMultiRow(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_MULTI_ROW).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByInsert(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByInsertWithPreparedStatementMultiRow(count: Int, orderNumber: String? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT_MULTI_ROW).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random(), orderNumber))
            }
            saver.commit()
        }

    }

    fun saveByInsertWithPreparedStatement(count: Int, orderNumber: String? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random(), orderNumber))
            }
            saver.commit()
        }

    }

    fun saveByInsertWithPreparedStatementAndUnnest(count: Int, orderNumber: String? = null) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT_UNNEST).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random(), orderNumber))
            }
            saver.commit()
        }

    }

    fun update(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.UPDATE).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(listId[i], currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun updatePreparedStatement(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        pdBatchByEntitySaverFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(listId[i], currencies.random(), accounts.random()))
            }
            saver.commit()
        }

    }

    fun saveByInsertAndPropertyMultiRow(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start collect insertion $count by property")

        pdBatchByPropertySaverFactory.getSaver(SaverType.INSERT_MULTI_ROW).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(null, currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end save insert collection $count by property")

    }

    fun updateByProperty(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start update $count by property")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(listId[i], currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end update collection $count by property")

    }

    fun updateByPropertyPreparedStatement(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, Any?>()

        log.info("start update $count by property prepared statement")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                fillRandomDataByKProperty(listId[i], currencies.random(), accounts.random(), data)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end update collection $count by property prepared statemen")

    }

    fun updateOnlyOneFieldByProperty(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, String?>()

        log.info("start update only one field $count by property")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE).use { saver ->
            for (i in 0 until count) {
                data[PaymentDocumentEntity::id] = listId[i].toString()
                data[PaymentDocumentEntity::prop10] = getRandomString(10)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end update only one field collection $count by property")

    }

    fun updateOnlyOneFieldByPropertyPreparedStatement(count: Int) {
        val listId = sqlHelper.getIdListForUpdate(count, PaymentDocumentEntity::class)
        val data = mutableMapOf<KMutableProperty1<PaymentDocumentEntity, *>, String?>()

        log.info("start update only one field $count by property prepared statement")

        pdBatchByPropertySaverFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            for (i in 0 until count) {
                data[PaymentDocumentEntity::id] = listId[i].toString()
                data[PaymentDocumentEntity::prop10] = getRandomString(10)
                saver.addDataForSave(data)
            }
            saver.commit()
        }

        log.info("end update only one field collection $count by property prepared statement")

    }

    fun updateOnlyOneFieldWithCommonCondition(orderNumber: String, prop10: String): Int {
        val connection = dataSource.connection

        log.info("start update only one field with common condition")

        val count = byPropertyProcessor.updateDataToDataBasePreparedStatement(
            PaymentDocumentEntity::class,
            setOf(PaymentDocumentEntity::prop10),
            listOf(listOf(prop10, orderNumber)),
            listOf("order_number"),
            connection
        )

        connection.close()

        log.info("end update only one field collection with common condition")

        return count

    }

    fun saveByInsertWithDropIndex(count: Int) {
        val currencies = currencyRepo.findAll()
        val accounts = accountRepo.findAll()

        log.info("start drop index before insertion $count")

        val scriptForCreateIndexes = sqlHelper.dropIndex(PaymentDocumentEntity::class)

        pdBatchByEntitySaverFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT_MULTI_ROW).use { saver ->
            for (i in 0 until count) {
                saver.addDataForSave(getRandomEntity(null, currencies.random(), accounts.random()))
            }
            saver.commit()
        }

        log.info("start create index after insertion $count")

        sqlHelper.executeScript(scriptForCreateIndexes)

        log.info("stop create index after insertion $count")

    }


    fun findAllByOrderNumberAndOrderDate(orderNumber: String, orderDate: LocalDate): List<PaymentDocumentEntity> {
        return pdCustomRepository.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
    }

    private fun getRandomEntity(
        id: Long?,
        cur: CurrencyEntity,
        account: AccountEntity,
        orderNumber: String? = null
    ): PaymentDocumentEntity {
        return PaymentDocumentEntity(
            orderDate = LocalDate.now(),
            orderNumber = orderNumber?: getRandomString(10),
            amount = BigDecimal.valueOf(Random.nextDouble()),
            cur = cur,
            expense = Random.nextBoolean(),
            account = account,
            paymentPurpose = getRandomString(100),
            prop10 = getRandomString(10),
            prop15 = getRandomString(15),
            prop20 = getRandomString(20),
        ).apply { this.id = id }
    }

    private fun fillRandomDataByKProperty(
        id: Long?,
        cur: CurrencyEntity,
        account: AccountEntity,
        data: MutableMap<KMutableProperty1<PaymentDocumentEntity, *>, Any?>
    ) {
        id?.let { data[PaymentDocumentEntity::id] = it }
        data[PaymentDocumentEntity::account] = account.id
        data[PaymentDocumentEntity::amount] = BigDecimal.valueOf(Random.nextDouble())
        data[PaymentDocumentEntity::expense] = Random.nextBoolean()
        data[PaymentDocumentEntity::cur] = cur.code
        data[PaymentDocumentEntity::orderDate] = LocalDate.now()
        data[PaymentDocumentEntity::orderNumber] = getRandomString(10)
        data[PaymentDocumentEntity::paymentPurpose] = getRandomString(100)
        data[PaymentDocumentEntity::prop10] = getRandomString(10)
        data[PaymentDocumentEntity::prop15] = getRandomString(15)
        data[PaymentDocumentEntity::prop20] = getRandomString(20)
    }
}