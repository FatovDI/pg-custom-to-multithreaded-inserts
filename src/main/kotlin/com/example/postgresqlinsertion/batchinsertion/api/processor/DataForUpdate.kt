package com.example.postgresqlinsertion.batchinsertion.api.processor

data class DataForUpdate(
    val tableName: String,
    val columns: Collection<String>,
    val conditions: Collection<String>,
    val data: Collection<Collection<Any?>>,
)
