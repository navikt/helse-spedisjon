package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import no.nav.helse.spedisjon.Melding.Companion.sha512
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
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
        val im = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `lagrer inntektsmelding bare en gang`(){
        val mediator = InntektsmeldingMediator(dataSource)
        val im = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `henter opp bare inntektsmeldinger som er timet ut og beriket`(){
        val b = lagreMeldingSendBehov(arkivreferanse = "b", timeout = LocalDateTime.now().minusMinutes(1))
        lagreMeldingSendBehov(arkivreferanse = "a", timeout = LocalDateTime.now().plusMinutes(1))
        lagreMeldingSendBehov(arkivreferanse = "c", timeout = LocalDateTime.now().minusMinutes(1))
        val berikelsesDao = BerikelseDao(dataSource)
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        berikelsesDao.behovBesvart(b, jacksonObjectMapper().createArrayNode())
        val metaInntektsmeldinger = inntektsmeldingDao.hentSendeklareMeldinger()
        assertEquals(1, metaInntektsmeldinger.size)
        assertEquals(b, metaInntektsmeldinger.first().originalMelding.duplikatkontroll())
    }

    @Test
    fun `teller en inntektsmelding`(){
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val mottatt = LocalDateTime.of(2022, 11, 3, 3, 3)
        inntektsmeldingDao.leggInn(
            melding = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a")),
            ønsketPublisert = mottatt.plusMinutes(5),
            mottatt = mottatt
        )
        assertEquals(1, inntektsmeldingDao.tellInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER, mottatt))
        assertEquals(0, inntektsmeldingDao.tellInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER, mottatt.plusSeconds(1)))
        assertEquals(1, inntektsmeldingDao.tellInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER, mottatt.minusSeconds(1)))
        assertEquals(0, inntektsmeldingDao.tellInntektsmeldinger("123", ORGNUMMER, mottatt.minusSeconds(1)))
        assertEquals(0, inntektsmeldingDao.tellInntektsmeldinger(FØDSELSNUMMER, "123", mottatt.minusSeconds(1)))
    }

    @Test
    fun `teller flere inntektsmeldinger`(){
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val mottatt = LocalDateTime.of(2022, 11, 3, 3, 3)
        inntektsmeldingDao.leggInn(Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a")), mottatt.plusMinutes(5), mottatt)
        inntektsmeldingDao.leggInn(Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "b")), mottatt.plusMinutes(5), mottatt.minusMinutes(1))
        inntektsmeldingDao.leggInn(Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "c")), mottatt.plusMinutes(5), mottatt)
        assertEquals(2, inntektsmeldingDao.tellInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER, mottatt))
    }

    @Test
    fun `lager json som inneholder berikelsesfelter og forventet flagg`(){
        val berikelse = """{ 
            "arbeidstakerAktorId": "a",
            "fødselsdato": "2022-01-01"
            }
        """.trimIndent()
        val sendeklarInntektsmelding = SendeklarInntektsmelding(
            "",
            "",
            Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a")),
            jacksonObjectMapper().readTree(berikelse),
            LocalDateTime.now()
        )
        val payload = sendeklarInntektsmelding.json(1)
        assertEquals(FØDSELSNUMMER, payload["arbeidstakerFnr"].asText())
        assertEquals(ORGNUMMER, payload["virksomhetsnummer"].asText())
        assertEquals("a", payload["arbeidstakerAktorId"].asText())
        assertEquals("2022-01-01", payload["fødselsdato"].asText())
        assertEquals("false", payload["harFlereInntektsmeldinger"].asText())
        val payload2 = sendeklarInntektsmelding.json(0)
        val payload3 = sendeklarInntektsmelding.json(2)
        assertEquals("false", payload2["harFlereInntektsmeldinger"].asText())
        assertEquals("true", payload3["harFlereInntektsmeldinger"].asText())
    }

    @Test
    fun `sendeklar inntektsmelding sorteringstest`(){
        val sendeKlareInntektsmeldinger = listOf(
            genererSendeklarInntektsmelding("andre", LocalDateTime.now()),
            genererSendeklarInntektsmelding("første", LocalDateTime.now().minusMinutes(1)),
            genererSendeklarInntektsmelding("tredje", LocalDateTime.now().plusMinutes(1))
            ).sorter()

        assertEquals(listOf("første", "andre", "tredje").map { it.sha512() }, sendeKlareInntektsmeldinger.map { it.originalMelding.duplikatkontroll() })
    }

    private fun antallInntektsmeldinger(fnr: String, orgnummer: String) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM inntektsmelding WHERE fnr = :fnr and orgnummer = :orgnummer", mapOf("fnr" to fnr, "orgnummer" to orgnummer)).map { row ->
                row.long(1)
            }.asSingle)
        }

    private fun genererSendeklarInntektsmelding(arkivreferanse: String, mottatt: LocalDateTime): SendeklarInntektsmelding {
        val berikelse = """{ 
            "arbeidstakerAktorId": "a",
            "fødselsdato": "2022-01-01"
            }
            """
        return SendeklarInntektsmelding(FØDSELSNUMMER, ORGNUMMER, Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = arkivreferanse)), jacksonObjectMapper().readTree(berikelse), mottatt)
    }

    private fun genererInntektsmelding(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arkivreferanse: String): JsonMessage {
        val inntektsmelding = """{
        "arbeidstakerFnr": "$fnr",
        "virksomhetsnummer": "$orgnummer",
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

    private fun lagreMeldingSendBehov(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arkivreferanse: String, timeout: LocalDateTime) : String{
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val meldingDao = MeldingDao(dataSource)
        val berikelsesDao = BerikelseDao(dataSource)
        val melding = Melding.Inntektsmelding(genererInntektsmelding(fnr, orgnummer, arkivreferanse))
        meldingDao.leggInn(melding)
        inntektsmeldingDao.leggInn(melding, timeout)
        berikelsesDao.behovEtterspurt(melding.fødselsnummer(), melding.duplikatkontroll(), listOf("aktørId"), LocalDateTime.now())
        return melding.duplikatkontroll()
    }

}