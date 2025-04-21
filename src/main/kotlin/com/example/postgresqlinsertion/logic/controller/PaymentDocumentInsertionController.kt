package com.example.postgresqlinsertion.logic.controller

import com.example.postgresqlinsertion.logic.dto.ResponseDto
import com.example.postgresqlinsertion.logic.service.PaymentDocumentService
import org.springframework.web.bind.annotation.*
import kotlin.system.measureTimeMillis

@RestController
@RequestMapping("/test-insertion")
class PaymentDocumentInsertionController(
    val service: PaymentDocumentService
) {

    @PostMapping("/copy/{count}")
    fun insertViaCopy(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopy(count)
        }
        return ResponseDto(
            name = "Copy method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-concurrent/{count}")
    fun insertViaCopyConcurrent(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyConcurrent(count)
        }
        return ResponseDto(
            name = "Copy concurrent method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-binary/{count}")
    fun insertViaCopyByBinary(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyBinary(count)
        }
        return ResponseDto(
            name = "Copy method by binary",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-property/{count}")
    fun insertViaCopyAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyAndKProperty(count)
        }
        return ResponseDto(
            name = "Copy method by property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-binary-and-property/{count}")
    fun insertViaCopyByBinaryAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyBinaryAndKProperty(count)
        }
        return ResponseDto(
            name = "Copy method by binary and property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-file/{count}")
    fun insertViaCopyByFile(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyViaFile(count)
        }
        return ResponseDto(
            name = "Copy method via file",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-binary-file/{count}")
    fun insertViaCopyByBinaryFile(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyViaBinaryFile(count)
        }
        return ResponseDto(
            name = "Copy method via binary file",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/copy-by-file-and-property/{count}")
    fun insertViaCopyByFileAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyAnpPropertyViaFile(count)
        }
        return ResponseDto(
            name = "Copy method via file and property",
            count = count,
            time = getTimeString(time)
        )
    }


    @PostMapping("/copy-by-binary-file-and-property/{count}")
    fun insertViaCopyByBinaryFileAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByCopyAnpPropertyViaBinaryFile(count)
        }
        return ResponseDto(
            name = "Copy method via binary file and property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert/{count}")
    fun insertViaInsert(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsert(count)
        }
        return ResponseDto(
            name = "Insert method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-prepared-statement/{count}")
    fun insertViaInsertWithPreparedStatement(
        @PathVariable count: Int,
        @RequestParam orderNumber: String? = null
    ): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertWithPreparedStatement(count, orderNumber)
        }
        return ResponseDto(
            name = "Insert method with prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-prepared-statement-unnest/{count}")
    fun insertViaInsertWithPreparedStatementUnnest(
        @PathVariable count: Int,
        @RequestParam orderNumber: String? = null
    ): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertWithPreparedStatementAndUnnest(count, orderNumber)
        }
        return ResponseDto(
            name = "Insert method with prepared statement and unnest",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update/{count}")
    fun update(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.update(count)
        }
        return ResponseDto(
            name = "Update method",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-prepared-statement/{count}")
    fun updatePreparedStatement(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updatePreparedStatement(count)
        }
        return ResponseDto(
            name = "Update method prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-by-property/{count}")
    fun insertViaInsertAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertAndProperty(count)
        }
        return ResponseDto(
            name = "Insert method by property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-by-property/{count}")
    fun updateViaInsertAndProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateByProperty(count)
        }
        return ResponseDto(
            name = "Update method by property",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-by-property-prepared-statement/{count}")
    fun updateViaInsertAndPropertyPreparedStatement(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateByPropertyPreparedStatement(count)
        }
        return ResponseDto(
            name = "Update method by property prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-only-one-field-by-property/{count}")
    fun updateOnlyOneFieldViaProperty(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateOnlyOneFieldByProperty(count)
        }
        return ResponseDto(
            name = "Update only one field by property with transaction",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-only-one-field-by-property-prepared-statement/{count}")
    fun updateOnlyOneFieldViaPropertyPreparedStatement(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.updateOnlyOneFieldByPropertyPreparedStatement(count)
        }
        return ResponseDto(
            name = "Update only one field by property with transaction prepared statement",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/update-only-one-field-with-common-condition/{orderNumber}")
    fun updateOnlyOneFieldWithCommonCondition(
        @PathVariable orderNumber: String,
        @RequestParam prop10: String
    ): ResponseDto {
        var count: Int
        val time = measureTimeMillis {
            count = service.updateOnlyOneFieldWithCommonCondition(orderNumber, prop10)
        }
        return ResponseDto(
            name = "Update only one field with common condition",
            count = count,
            time = getTimeString(time)
        )
    }

    @PostMapping("/insert-with-drop-index/{count}")
    fun insertViaInsertWithDropIndex(@PathVariable count: Int): ResponseDto {
        val time = measureTimeMillis {
            service.saveByInsertWithDropIndex(count)
        }
        return ResponseDto(
            name = "Insert method with drop index",
            count = count,
            time = getTimeString(time)
        )
    }


    private fun getTimeString(time: Long):String {
        val min = (time / 1000) / 60
        val sec = (time / 1000) % 60
        val ms = time - min*1000*60 - sec*1000
        return "$min min, $sec sec $ms ms"
    }
}