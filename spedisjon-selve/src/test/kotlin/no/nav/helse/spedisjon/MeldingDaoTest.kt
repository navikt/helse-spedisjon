package no.nav.helse.spedisjon

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.UUID
import javax.sql.DataSource

class MeldingDaoTest {

    @Test
    fun `hente meldinger - tom liste`() = databaseTest {
        val dao = MeldingDao(it)
        assertEquals(emptyList<Any>(), dao.hentMeldinger(emptyList()))
    }

    @Test
    fun `hente meldinger`() = databaseTest {
        val dao = MeldingDao(it)
        val nyMeldingDto = NyMeldingDto(
            type = "ny_søknad",
            fnr = "fnr",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatkontroll = "unik_nøkkel",
            jsonBody = "{}"
        )
        val internDokumentId = dao.leggInn(nyMeldingDto) ?: fail { "forventet å sette inn melding" }
        val actual = dao.hentMeldinger(listOf(internDokumentId)).single()
        assertEquals(MeldingDto(
            type = nyMeldingDto.type,
            fnr = nyMeldingDto.fnr,
            internDokumentId = internDokumentId,
            eksternDokumentId = nyMeldingDto.eksternDokumentId,
            rapportertDato = LocalDateTime.MIN,
            duplikatkontroll = nyMeldingDto.duplikatkontroll.padEnd(128, ' '),
            jsonBody = nyMeldingDto.jsonBody
        ), actual.copy(
            rapportertDato = LocalDateTime.MIN
        ))
    }

    private fun databaseTest(testblokk: (DataSource) -> Unit) {
        val testDataSource = databaseContainer.nyTilkobling()
        try {
            testblokk(testDataSource.ds)
        } finally {
            databaseContainer.droppTilkobling(testDataSource)
        }
    }
}