package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.runs
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageContext
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import no.nav.helse.spedisjon.Melding.Companion.sha512
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime

class InntektsmeldingTest : AbstractDatabaseTest() {

    @Test
    fun `tar imot inntektsmelding`() {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), mockk())
        val mediator = InntektsmeldingMediator(dataSource, mockk(), meldingMediator = meldingMediator)
        val im = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `lagrer inntektsmelding bare en gang`() {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), mockk())
        val mediator = InntektsmeldingMediator(dataSource, mockk(), meldingMediator = meldingMediator)
        val im = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `henter opp bare inntektsmeldinger som er timet ut og beriket`() {
        val speedClient = mockSpeed(
            fødselsdato = LocalDate.parse("2022-01-01"),
            aktørId = "b"
        )
        val b = lagreMelding(arkivreferanse = "b", timeout = LocalDateTime.now().minusMinutes(1))
        lagreMelding(arkivreferanse = "a", timeout = LocalDateTime.now().plusMinutes(1))
        lagreMelding(arkivreferanse = "c", timeout = LocalDateTime.now().minusMinutes(1))
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val metaInntektsmeldinger = sessionOf(dataSource).use {
            it.transaction{
                inntektsmeldingDao.hentSendeklareMeldinger(it, speedClient)
            }
        }
        assertEquals(2, metaInntektsmeldinger.size)
        assertEquals(b, metaInntektsmeldinger.first().originalMelding.duplikatkontroll())
    }

    @Test
    fun `teller en inntektsmelding`() {
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val mottatt = LocalDateTime.of(2022, 11, 3, 3, 3)
        inntektsmeldingDao.leggInn(
            melding = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a", arbeidsforholdId = "noe")),
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
    fun `teller flere inntektsmeldinger`() {
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val mottatt = LocalDateTime.of(2022, 11, 3, 3, 3)
        inntektsmeldingDao.leggInn(Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a", arbeidsforholdId = "noe")), mottatt.plusMinutes(5), mottatt)
        inntektsmeldingDao.leggInn(Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "b", arbeidsforholdId = "noe")), mottatt.plusMinutes(5), mottatt.minusMinutes(1))
        inntektsmeldingDao.leggInn(Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "c", arbeidsforholdId = "noe")), mottatt.plusMinutes(5), mottatt)
        assertEquals(2, inntektsmeldingDao.tellInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER, mottatt))
    }

    @Test
    fun `lager json som inneholder berikelsesfelter og forventet flagg`() {
        val speedClient = mockSpeed(
            fødselsdato = LocalDate.parse("2022-01-01"),
            aktørId = "a"
        )
        val berikelse = Berikelse(LocalDate.parse("2022-01-01"), null, "a", emptyList())
        val sendeklarInntektsmelding = SendeklarInntektsmelding(
            "",
            "",
            Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a")),
            berikelse,
            LocalDateTime.now()
        )

        val dao = mockk<InntektsmeldingDao> {
            every { tellInntektsmeldinger(any(), any(), any()) } returns 0 andThen 1 andThen 2
            every { markerSomEkspedert(any(), any()) } just runs
        }
        val messageContext = object : MessageContext {
            lateinit var forrigeMelding: JsonNode
            override fun publish(message: String) {
                TODO("Not yet implemented")
            }

            override fun publish(key: String, message: String) {
                val payload = jacksonObjectMapper().readTree(message)!!
                forrigeMelding = payload
            }

            override fun rapidName(): String {
                TODO("Not yet implemented")
            }
        }
        sendeklarInntektsmelding.send(dao, messageContext, 0, mockk(relaxed = true))
        messageContext.forrigeMelding.also { payload ->
            assertEquals(FØDSELSNUMMER, payload["arbeidstakerFnr"].asText())
            assertEquals(ORGNUMMER, payload["virksomhetsnummer"].asText())
            assertEquals("a", payload["arbeidstakerAktorId"].asText())
            assertEquals("2022-01-01", payload["fødselsdato"].asText())
            assertEquals("false", payload["harFlereInntektsmeldinger"].asText())
        }

        sendeklarInntektsmelding.send(dao, messageContext, 0, mockk(relaxed = true))
        messageContext.forrigeMelding.also { payload ->
            assertEquals(FØDSELSNUMMER, payload["arbeidstakerFnr"].asText())
            assertEquals(ORGNUMMER, payload["virksomhetsnummer"].asText())
            assertEquals("a", payload["arbeidstakerAktorId"].asText())
            assertEquals("2022-01-01", payload["fødselsdato"].asText())
            assertEquals("false", payload["harFlereInntektsmeldinger"].asText())
        }

        sendeklarInntektsmelding.send(dao, messageContext, 0, mockk(relaxed = true))
        messageContext.forrigeMelding.also { payload ->
            assertEquals(FØDSELSNUMMER, payload["arbeidstakerFnr"].asText())
            assertEquals(ORGNUMMER, payload["virksomhetsnummer"].asText())
            assertEquals("a", payload["arbeidstakerAktorId"].asText())
            assertEquals("2022-01-01", payload["fødselsdato"].asText())
            assertEquals("true", payload["harFlereInntektsmeldinger"].asText())
        }
    }

    @Test
    fun `sendeklar inntektsmelding sorteringstest`() {
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
        val berikelse = Berikelse(LocalDate.parse("2022-01-01"), null, "a", emptyList())
        return SendeklarInntektsmelding(FØDSELSNUMMER, ORGNUMMER, Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = arkivreferanse)), berikelse, mottatt)
    }

    private val registry = SimpleMeterRegistry()
    private fun genererInntektsmelding(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arbeidsforholdId: String? = null, arkivreferanse: String): JsonMessage {
        val arbeidsforholdIdJson = if (arbeidsforholdId == null) "" else """ "arbeidsforholdId": "$arbeidsforholdId", """
        val inntektsmelding = """{
        "arbeidstakerFnr": "$fnr",
        "virksomhetsnummer": "$orgnummer",
        "mottattDato": "${LocalDateTime.now()}",
        $arbeidsforholdIdJson
        "arkivreferanse": "$arkivreferanse"
        }"""

        return JsonMessage(inntektsmelding, MessageProblems(inntektsmelding), registry).also {
            it.interestedIn("arbeidstakerFnr")
            it.interestedIn("virksomhetsnummer")
            it.interestedIn("mottattDato")
            it.interestedIn("arkivreferanse")
            it.interestedIn("arbeidsforholdId")
        }
    }

    private fun lagreMelding(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arbeidsforholdId: String? = null, arkivreferanse: String, timeout: LocalDateTime) : String{
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val meldingDao = MeldingDao(dataSource)
        val melding = Melding.Inntektsmelding(genererInntektsmelding(fnr, orgnummer, arbeidsforholdId, arkivreferanse))
        meldingDao.leggInn(melding)
        inntektsmeldingDao.leggInn(melding, timeout)
        return melding.duplikatkontroll()
    }

}