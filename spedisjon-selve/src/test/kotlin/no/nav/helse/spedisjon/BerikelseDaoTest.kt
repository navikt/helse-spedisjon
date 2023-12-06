package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.spedisjon.Melding.Companion.sha512
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.*
import javax.sql.DataSource

internal class BerikelseDaoTest: AbstractRiverTest() {

    @Test
    fun `ikke finn ting som er beriket`() {
        val dao = BerikelseDao(dataSource)
        dao.behovEtterspurt("fnr", "duplikatkontroll", listOf("aktørId", "fødselsdato"), LocalDateTime.now())
        dao.behovBesvart("duplikatkontroll", enLøsning) {}
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
    fun `ubesvart behov inneholder nødvendig informasjon`() {
        val dao = BerikelseDao(dataSource)
        val opprettet = LocalDateTime.parse("2022-08-11T00:15:00.600")
        dao.behovEtterspurt("fnr", "duplikatkontroll", listOf("aktørId", "fødselsdato"), opprettet)
        assertEquals(listOf(UbesvartBehov("fnr", "duplikatkontroll".padEnd(128, ' '), listOf("aktørId", "fødselsdato"), opprettet)), dao.ubesvarteBehov(LocalDateTime.now()))
    }

    @Test
    fun `markeres som besvart når vi mottar løsning`() {
        val duplikatkontroll = "${UUID.randomUUID()}".sha512()
        val dao = BerikelseDao(dataSource)
        dao.behovEtterspurt("fnr", duplikatkontroll, listOf("aktørId", "fødselsdato"), LocalDateTime.now())
        assertFalse(dao.behovErBesvart(duplikatkontroll))
        dao.behovBesvart(duplikatkontroll, enLøsning) {}
        assertTrue(dao.behovErBesvart(duplikatkontroll))
    }

    @Test
    fun `markere når behov er etterspurt og besvart`() {
        val duplikatkontroll = "${UUID.randomUUID()}".sha512()
        val dao = BerikelseDao(dataSource)
        assertFalse(dao.behovErEtterspurt(duplikatkontroll))
        assertFalse(dao.behovErBesvart(duplikatkontroll))
        dao.behovEtterspurt("fnr", duplikatkontroll, listOf("foo"), LocalDateTime.now())
        assertTrue(dao.behovErEtterspurt(duplikatkontroll))
        assertFalse(dao.behovErBesvart(duplikatkontroll))
        dao.behovBesvart(duplikatkontroll, enLøsning) {}
        assertTrue(dao.behovErEtterspurt(duplikatkontroll))
        assertTrue(dao.behovErBesvart(duplikatkontroll))
    }

    @Test
    fun `markerer uløste behov som er gamle (eldre enn 3 timer)`() {
        assertFalse("1".ubesvartBehov(LocalDateTime.now()).gammelt)
        assertTrue("2".ubesvartBehov(LocalDateTime.now().minusHours(3).minusSeconds(1)).gammelt)
        assertFalse("3".ubesvartBehov(LocalDateTime.now().minusHours(3).plusSeconds(2)).gammelt)
        assertTrue("4".ubesvartBehov(LocalDateTime.now().minusDays(5)).gammelt)
        assertFalse("5".ubesvartBehov(LocalDateTime.now().plusDays(5)).gammelt)
    }

    override fun createRiver(rapidsConnection: RapidsConnection, dataSource: DataSource) {}

    private companion object {
        private val enLøsning = jacksonObjectMapper().readTree("""{"foo": "bar"}""")
        private fun String.ubesvartBehov(opprettet: LocalDateTime) = UbesvartBehov("fnr", this, emptyList(), opprettet)
    }
}