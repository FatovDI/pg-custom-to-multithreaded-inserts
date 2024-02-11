package com.example.postgresqlinsertion.batchinsertion.api.factory

enum class SaverType {
    COPY,
    COPY_BINARY,
    COPY_VIA_FILE,
    COPY_BINARY_VIA_FILE,
    INSERT,
    INSERT_PREPARED_STATEMENT,
    UPDATE,
    UPDATE_PREPARED_STATEMENT,
}
