package com.example.postgresqlinsertion

import org.junit.jupiter.api.BeforeAll
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer

@SpringBootTest
abstract class AbstractTestcontainersIntegrationTest {

    companion object {

        private val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:14.6-alpine")
            .apply {
                this.withCommand("postgres", "-c", "max_prepared_transactions=100")
            }

        @JvmStatic
        @DynamicPropertySource
        fun properties(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url", postgres::getJdbcUrl)
            registry.add("spring.datasource.username", postgres::getUsername)
            registry.add("spring.datasource.password", postgres::getPassword)
        }

        @JvmStatic
        @BeforeAll
        internal fun setUp() {
            postgres.start()
        }
    }

}