package no.nav.helse.spedisjon.api

import java.util.*
import javax.sql.DataSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

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
                duplikatkontroll = nyMeldingDto.duplikatkontroll.padEnd(128, ' '),
                jsonBody = nyMeldingDto.jsonBody
            ), actual)
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
