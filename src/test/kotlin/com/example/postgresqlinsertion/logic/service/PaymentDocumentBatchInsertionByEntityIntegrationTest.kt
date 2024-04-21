package com.example.postgresqlinsertion.logic.service

import com.example.postgresqlinsertion.batchinsertion.api.factory.BatchInsertionByEntityFactory
import com.example.postgresqlinsertion.batchinsertion.api.factory.SaverType
import com.example.postgresqlinsertion.logic.entity.PaymentDocumentEntity
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.time.LocalDate

@RunWith(SpringRunner::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@TestPropertySource(locations = ["classpath:application-test.properties"])
internal class PaymentDocumentBatchInsertionByEntityIntegrationTest {

    @Autowired
    lateinit var batchInsertionFactory: BatchInsertionByEntityFactory<PaymentDocumentEntity>

    @Autowired
    lateinit var service: PaymentDocumentService

    @BeforeEach
    fun setUp() {
    }

    @Test
    fun `save entity via copy method`() {
        val orderNumber = "111"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save entity via copy method"

        batchInsertionFactory.getSaver(SaverType.COPY).use { saver ->

           saver.addDataForSave(
               PaymentDocumentEntity(
                   paymentPurpose = paymentPurpose,
                   orderNumber = orderNumber,
                   orderDate = orderDate,
                   prop15 = "END"
               )
           )
           saver.commit()
       }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isGreaterThan(0)
        assertThat(savedPd.first().paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save several entity via copy method`() {
        val orderNumber = "222"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save several entity via copy method"

        batchInsertionFactory.getSaver(SaverType.COPY).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)
        assertThat(savedPd[0].paymentPurpose).isEqualTo(paymentPurpose)
        assertThat(savedPd[1].paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save entity via insert method`() {
        val orderNumber = "333"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save entity via insert method"

        batchInsertionFactory.getSaver(SaverType.INSERT).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isGreaterThan(0)
        assertThat(savedPd.first().paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save several entity via insert method`() {
        val orderNumber = "444"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save several entity via insert method"

        batchInsertionFactory.getSaver(SaverType.INSERT).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)
        assertThat(savedPd[0].paymentPurpose).isEqualTo(paymentPurpose)
        assertThat(savedPd[1].paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save entity via insert method with prepared statement`() {
        val orderNumber = "333_PS"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save entity via insert method with prepared statement"

        batchInsertionFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isGreaterThan(0)
        assertThat(savedPd.first().paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save several entity via insert method with prepared statement`() {
        val orderNumber = "444_PS"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save several entity via insert method with prepared statement"

        batchInsertionFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)
        assertThat(savedPd[0].paymentPurpose).isEqualTo(paymentPurpose)
        assertThat(savedPd[1].paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save entity via insert method with prepared statement and unnest`() {
        val orderNumber = "333_PS_UN"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save entity via insert method with prepared statement and unnest"

        batchInsertionFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT_UNNEST).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isGreaterThan(0)
        assertThat(savedPd.first().paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save several entity via insert method with prepared statement and unnest`() {
        val orderNumber = "444_PS_UN"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save several entity via insert method with prepared statement and unnest"

        batchInsertionFactory.getSaver(SaverType.INSERT_PREPARED_STATEMENT_UNNEST).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)
        assertThat(savedPd[0].paymentPurpose).isEqualTo(paymentPurpose)
        assertThat(savedPd[1].paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `update entity via insert method`() {
        val orderNumber = "555"
        val orderDate = LocalDate.now()
        val paymentPurposeIns = "save entity via insert method"
        val paymentPurposeUpd = "update entity via insert method"
        batchInsertionFactory.getSaver(SaverType.INSERT).use { saver ->
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurposeIns,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }
        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(1)

        batchInsertionFactory.getSaver(SaverType.UPDATE).use { saver ->
            saver.addDataForSave(savedPd.first().apply { paymentPurpose = paymentPurposeUpd })
            saver.commit()
        }

        val updatedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(updatedPd.size).isGreaterThan(0)
        assertThat(updatedPd.first().paymentPurpose).isEqualTo(paymentPurposeUpd)
    }

    @Test
    fun `update several entity via insert method`() {
        val orderNumber = "666"
        val orderDate = LocalDate.now()
        val paymentPurposeIns = "save several entity via insert method"
        val paymentPurposeUpd = "update several entity via insert method"
        batchInsertionFactory.getSaver(SaverType.INSERT).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurposeIns,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurposeIns,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }
        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)

        batchInsertionFactory.getSaver(SaverType.UPDATE).use { saver ->
            saver.addDataForSave(savedPd[0].apply { paymentPurpose = paymentPurposeUpd })
            saver.addDataForSave(savedPd[1].apply { paymentPurpose = paymentPurposeUpd })
            saver.saveData()
            saver.commit()
        }

        val updatedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(updatedPd.size).isEqualTo(2)
        assertThat(updatedPd[0].paymentPurpose).isEqualTo(paymentPurposeUpd)
        assertThat(updatedPd[1].paymentPurpose).isEqualTo(paymentPurposeUpd)
    }

    @Test
    fun `update entity via insert method prepared statement`() {
        val orderNumber = "555_ps"
        val orderDate = LocalDate.now()
        val paymentPurposeIns = "save entity via insert method prepared statement"
        val paymentPurposeUpd = "update entity via insert method prepared statement"
        batchInsertionFactory.getSaver(SaverType.INSERT).use { saver ->
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurposeIns,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }
        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(1)

        batchInsertionFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            saver.addDataForSave(savedPd.first().apply { paymentPurpose = paymentPurposeUpd })
            saver.commit()
        }

        val updatedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(updatedPd.size).isGreaterThan(0)
        assertThat(updatedPd.first().paymentPurpose).isEqualTo(paymentPurposeUpd)
    }

    @Test
    fun `update several entity via insert method prepared statement`() {
        val orderNumber = "666_ps"
        val orderDate = LocalDate.now()
        val paymentPurposeIns = "save several entity via insert method prepared statement"
        val paymentPurposeUpd = "update several entity via insert method prepared statement"
        batchInsertionFactory.getSaver(SaverType.INSERT).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurposeIns,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurposeIns,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }
        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)

        batchInsertionFactory.getSaver(SaverType.UPDATE_PREPARED_STATEMENT).use { saver ->
            saver.addDataForSave(savedPd[0].apply { paymentPurpose = paymentPurposeUpd })
            saver.addDataForSave(savedPd[1].apply { paymentPurpose = paymentPurposeUpd })
            saver.saveData()
            saver.commit()
        }

        val updatedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(updatedPd.size).isEqualTo(2)
        assertThat(updatedPd[0].paymentPurpose).isEqualTo(paymentPurposeUpd)
        assertThat(updatedPd[1].paymentPurpose).isEqualTo(paymentPurposeUpd)
    }

    @Test
    fun `save entity via copy method by binary file`() {
        val orderNumber = "777"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save entity via copy method by binary file"

        batchInsertionFactory.getSaver(SaverType.COPY_BINARY_VIA_FILE).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()

        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isGreaterThan(0)
        assertThat(savedPd.first().paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save several entity via copy method by binary file`() {
        val orderNumber = "888"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save several entity via copy method by binary file"

        batchInsertionFactory.getSaver(SaverType.COPY_BINARY_VIA_FILE).use { saver ->
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)
        assertThat(savedPd[0].paymentPurpose).isEqualTo(paymentPurpose)
        assertThat(savedPd[1].paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save entity via copy method by binary`() {
        val orderNumber = "771"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save entity via copy method by binary"

        batchInsertionFactory.getSaver(SaverType.COPY_BINARY).use { saver ->

            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()

        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isGreaterThan(0)
        assertThat(savedPd.first().paymentPurpose).isEqualTo(paymentPurpose)
    }

    @Test
    fun `save several entity via copy method by binary`() {
        val orderNumber = "881"
        val orderDate = LocalDate.now()
        val paymentPurpose = "save several entity via copy method by binary"

        batchInsertionFactory.getSaver(SaverType.COPY_BINARY).use { saver ->
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.addDataForSave(
                PaymentDocumentEntity(
                    paymentPurpose = paymentPurpose,
                    orderNumber = orderNumber,
                    orderDate = orderDate,
                    prop15 = "END"
                )
            )
            saver.commit()
        }

        val savedPd = service.findAllByOrderNumberAndOrderDate(orderNumber, orderDate)
        assertThat(savedPd.size).isEqualTo(2)
        assertThat(savedPd[0].paymentPurpose).isEqualTo(paymentPurpose)
        assertThat(savedPd[1].paymentPurpose).isEqualTo(paymentPurpose)
    }

}
