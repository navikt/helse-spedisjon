package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InntektsmeldingTest : AbstractDatabaseTest() {

    @Test
    fun `tar imot inntektsmelding`(){
        val mediator = InntektsmeldingMediator(dataSource)
        val im = Melding.Inntektsmelding(genererInntektsmelding("a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `lagrer inntektsmelding bare en gang`(){
        val mediator = InntektsmeldingMediator(dataSource)
        val im = Melding.Inntektsmelding(genererInntektsmelding("a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `henter opp bare inntektsmeldinger som er timet ut og beriket`(){
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val meldingDao = MeldingDao(dataSource)
        val berikelsesDao = BerikelseDao(dataSource)
        val a = Melding.Inntektsmelding(genererInntektsmelding("a"))
        val b = Melding.Inntektsmelding(genererInntektsmelding("b"))
        val c = Melding.Inntektsmelding(genererInntektsmelding("c"))
        meldingDao.leggInn(a)
        meldingDao.leggInn(b)
        meldingDao.leggInn(c)
        inntektsmeldingDao.leggInn(a, LocalDateTime.now().plusMinutes(1))
        inntektsmeldingDao.leggInn(b, LocalDateTime.now().minusMinutes(1))
        inntektsmeldingDao.leggInn(c, LocalDateTime.now().minusMinutes(1))
        berikelsesDao.behovEtterspurt(a.fødselsnummer(), a.duplikatkontroll(), listOf("aktørId"), LocalDateTime.now())
        berikelsesDao.behovEtterspurt(b.fødselsnummer(), b.duplikatkontroll(), listOf("aktørId"), LocalDateTime.now())
        berikelsesDao.behovEtterspurt(c.fødselsnummer(), c.duplikatkontroll(), listOf("aktørId"), LocalDateTime.now())
        berikelsesDao.behovBesvart(b.duplikatkontroll(), jacksonObjectMapper().createArrayNode())
        val metaInntektsmeldinger = inntektsmeldingDao.hentUsendteMeldinger()
        assertEquals(1, metaInntektsmeldinger.size)
        assertEquals(b.duplikatkontroll(), metaInntektsmeldinger.first().originalMelding.duplikatkontroll())
    }

    private fun antallInntektsmeldinger(fnr: String, orgnummer: String) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM inntektsmelding WHERE fnr = :fnr and orgnummer = :orgnummer", mapOf("fnr" to fnr, "orgnummer" to orgnummer)).map { row ->
                row.long(1)
            }.asSingle)
        }

    private fun genererInntektsmelding(arkivreferanse: String): JsonMessage {
        val inntektsmelding = """{
        "arbeidstakerFnr": "$FØDSELSNUMMER",
        "virksomhetsnummer": "$ORGNUMMER",
        "mottattDato": "${LocalDateTime.now()}",
        "arkivreferanse": "$arkivreferanse"
        }"""

        return JsonMessage(inntektsmelding, MessageProblems(inntektsmelding)).also {
            it.interestedIn("arbeidstakerFnr")
            it.interestedIn("virksomhetsnummer")
            it.interestedIn("mottattDato")
            it.interestedIn("arkivreferanse")
        }
    }
}