package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.RapidsConnection
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import javax.sql.DataSource

internal class BerikelseDaoTest: AbstractRiverTest() {

    @Test
    fun `ikke finn ting som er beriket`() {
        val dao = BerikelseDao(dataSource)
        dao.behovEtterspurt("fnr", "duplikatkontroll", listOf("aktørId", "fødselsdato"), LocalDateTime.now())
        dao.behovBesvart("duplikatkontroll", jacksonObjectMapper().readTree("{}"))
        assertEquals(0, dao.ubesvarteBehov(LocalDateTime.now()).size)
    }

    @Test
    fun `finn ting som ikke er beriket`() {
        val dao = BerikelseDao(dataSource)
        dao.behovEtterspurt("fnr", "duplikatkontroll", listOf("aktørId", "fødselsdato"), LocalDateTime.now().minusMinutes(1))
        assertEquals(1, dao.ubesvarteBehov(LocalDateTime.now()).size)
    }

    @Test
    fun `ikke finn ting som ikke har timet ut`() {
        val dao = BerikelseDao(dataSource)
        dao.behovEtterspurt("fnr", "duplikatkontroll", listOf("aktørId", "fødselsdato"), LocalDateTime.now().minusMinutes(10))
        assertEquals(0, dao.ubesvarteBehov(LocalDateTime.now().minusMinutes(11)).size)
    }

    @Test
    fun `ubsevart behov inneholder nødvendig informasjon`() {
        val dao = BerikelseDao(dataSource)
        dao.behovEtterspurt("fnr", "duplikatkontroll", listOf("aktørId", "fødselsdato"), LocalDateTime.now().minusMinutes(1))
        assertEquals(listOf(UbesvartBehov("fnr", "duplikatkontroll".padEnd(128, ' '), listOf("aktørId", "fødselsdato"))), dao.ubesvarteBehov(LocalDateTime.now()))
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {}
}