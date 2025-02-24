package no.nav.helse.spedisjon.api

import com.github.navikt.tbd_libs.test_support.TestDataSource
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*

internal class DuplikateMeldingerTest {

    private lateinit var meldingDao: MeldingDao
    private lateinit var testDataSource: TestDataSource

    @BeforeEach
    fun setup() {
        testDataSource = databaseContainer.nyTilkobling()
        meldingDao = MeldingDao(testDataSource.ds)
    }

    @AfterEach
    fun tearDown() {
        databaseContainer.droppTilkobling(testDataSource)
    }

    @Test
    fun `duplikat inntektsmelding slipper ikke igjennom`() {
        val duplikatnøkkel = "unik nøkkel"
        val im1 = NyMeldingDto(
            type = "inntektsmelding",
            fnr = "123",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatkontroll = duplikatnøkkel,
            jsonBody = "{}"
        )
        val im2 = NyMeldingDto(
            type = "inntektsmelding",
            fnr = "567",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatkontroll = duplikatnøkkel,
            jsonBody = "{}"
        )
        assertEquals(MeldingDao.Resultat.Utfall.SATT_INN_NY, meldingDao.leggInn(im1).utfall)
        assertEquals(MeldingDao.Resultat.Utfall.HENTET_EKSISTERENDE, meldingDao.leggInn(im2).utfall)
    }
}
