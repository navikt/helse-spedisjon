package no.nav.helse.spedisjon.api

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
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
        val result = dao.leggInn(nyMeldingDto)
        val actual = dao.hentMeldinger(listOf(result.internId)).single()
        assertEquals(
            MeldingDto(
                type = nyMeldingDto.type,
                fnr = nyMeldingDto.fnr,
                internDokumentId = result.internId,
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