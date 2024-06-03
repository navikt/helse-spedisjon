package no.nav.helse.opprydding

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers
import com.github.navikt.tbd_libs.test_support.TestDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

val databaseContainer = DatabaseContainers.container("spedisjon-opprydding", CleanupStrategy.tables("inntektsmelding,berikelse,melding"))


internal abstract class DataSourceBuilderTest {
    protected lateinit var testDataSource: TestDataSource

    @BeforeEach
    fun setup() {
        testDataSource = databaseContainer.nyTilkobling()
    }

    @AfterEach
    fun teardown() {
        databaseContainer.droppTilkobling(testDataSource)
    }
}