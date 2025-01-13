package no.nav.helse.spedisjon.async

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID

class EkspederingDaoTest {

    @Test
    fun `duplikat melding`() {
        val testDataSource = databaseContainer.nyTilkobling()
        try {
            val dao = EkspederingDao(testDataSource::ds)
            val id = UUID.randomUUID()
            assertTrue(dao.meldingEkspedert(id))
            assertFalse(dao.meldingEkspedert(id))
        } finally {
            databaseContainer.droppTilkobling(testDataSource)
        }
    }
}
