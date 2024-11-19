package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.test_support.TestDataSource
import no.nav.helse.spedisjon.Meldingsdetaljer.Companion.sha512
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertEquals
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
        val im1 = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = "123",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf(duplikatnøkkel),
            jsonBody = "{}"
        )
        val im2 = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = "567",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf(duplikatnøkkel),
            jsonBody = "{}"
        )
        assertNotNull(meldingDao.leggInn(im1))
        assertNull(meldingDao.leggInn(im2))
    }

    @Test
    fun `duplikatkontroll`() {
        val detaljer = Meldingsdetaljer(
            type = "ny_søknad",
            fnr = "123",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf("bit_a", "bit_b"),
            jsonBody = "{}"
        )
        assertEquals("bit_abit_b".sha512(), detaljer.duplikatkontroll)
        assertEquals("56cf3c91019a0d437e2ff6f735ab95ab6239d93a11a0841589b981ce337a4459dcece37b6b06baa2dbc9884354793db462af5c4adf4cc97749fe16b50ecf5ae5", detaljer.duplikatkontroll)
    }
}
