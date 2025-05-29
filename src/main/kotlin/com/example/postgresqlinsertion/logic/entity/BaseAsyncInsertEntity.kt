package com.example.postgresqlinsertion.logic.entity

import java.util.UUID
import javax.persistence.Entity
import javax.persistence.Inheritance
import javax.persistence.InheritanceType

@Entity
@Inheritance(strategy = InheritanceType.TABLE_PER_CLASS)
abstract class BaseAsyncInsertEntity : BaseEntity() {

    var transactionId: UUID? = null

}