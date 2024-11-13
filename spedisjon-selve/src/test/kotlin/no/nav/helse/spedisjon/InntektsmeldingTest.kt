package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.mockk.mockk
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.spedisjon.Melding.Companion.sha512
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class InntektsmeldingTest : AbstractDatabaseTest() {

    @Test
    fun `tar imot inntektsmelding`() {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), mockk(), mockk(relaxed = true))
        val mediator = InntektsmeldingMediator(dataSource, mockk(), meldingMediator = meldingMediator, dokumentAliasProducer = mockk(relaxed = true))
        val im = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `lagrer inntektsmelding bare en gang`() {
        val meldingMediator = MeldingMediator(MeldingDao(dataSource), mockk(), mockk(relaxed = true))
        val mediator = InntektsmeldingMediator(dataSource, mockk(), meldingMediator = meldingMediator, dokumentAliasProducer = mockk(relaxed = true))
        val im = Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = "a"))
        mediator.lagreInntektsmelding(im, TestRapid())
        mediator.lagreInntektsmelding(im, TestRapid())
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `henter opp bare inntektsmeldinger som er timet ut`() {
        val b = lagreMelding(arkivreferanse = "b", ønsketPublisert = LocalDateTime.now().minusMinutes(1))
        lagreMelding(arkivreferanse = "a", ønsketPublisert = LocalDateTime.now().plusMinutes(1))
        lagreMelding(arkivreferanse = "c", ønsketPublisert = LocalDateTime.now().minusMinutes(1))
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val metaInntektsmeldinger = sessionOf(dataSource).use {
            buildList {
                inntektsmeldingDao.hentSendeklareMeldinger(0) { im, _ ->
                    add(im)
                }
            }
        }
        assertEquals(2, metaInntektsmeldinger.size)
        assertEquals(b, metaInntektsmeldinger.first().melding.duplikatkontroll())
    }

    @Test
    fun `teller flere inntektsmeldinger`() {
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val mottatt = LocalDateTime.of(2022, 11, 3, 3, 3)
        val ønsetPublisert = LocalDateTime.now().minusHours(1)

        lagreMelding(arkivreferanse = "a", arbeidsforholdId = "noe", ønsketPublisert = ønsetPublisert, mottatt = mottatt)
        lagreMelding(arkivreferanse = "b", arbeidsforholdId = "noe", ønsketPublisert = ønsetPublisert, mottatt = mottatt.minusMinutes(1))
        lagreMelding(arkivreferanse = "c", arbeidsforholdId = "noe", ønsketPublisert = ønsetPublisert, mottatt = mottatt)

        val inntektsmeldinger = tellInntektsmeldinger(inntektsmeldingDao, 0)
        assertEquals(3, inntektsmeldinger.size)
        inntektsmeldinger[0].also { (im, antallMottatt) ->
            assertEquals(mottatt.minusMinutes(1), im.mottatt)
            assertEquals(3, antallMottatt)
        }
        inntektsmeldinger[1].also { (im, antallMottatt) ->
            assertEquals(mottatt, im.mottatt)
            assertEquals(2, antallMottatt)
        }
        inntektsmeldinger[2].also { (im, antallMottatt) ->
            assertEquals(mottatt, im.mottatt)
            assertEquals(2, antallMottatt)
        }
    }

    private fun tellInntektsmeldinger(inntektsmeldingDao: InntektsmeldingDao, timeout: Long) =
        sessionOf(dataSource).use {
            buildList {
                inntektsmeldingDao.hentSendeklareMeldinger(timeout) { im, antall ->
                    add(im to antall)
                }
            }
        }

    @Test
    fun `sendeklar inntektsmelding sorteringstest`() {
        val sendeKlareInntektsmeldinger = listOf(
            genererSendeklarInntektsmelding("andre", LocalDateTime.now()),
            genererSendeklarInntektsmelding("første", LocalDateTime.now().minusMinutes(1)),
            genererSendeklarInntektsmelding("tredje", LocalDateTime.now().plusMinutes(1))
        ).sorter()

        assertEquals(listOf("første", "andre", "tredje").map { it.sha512() }, sendeKlareInntektsmeldinger.map { it.melding.duplikatkontroll() })
    }

    private fun antallInntektsmeldinger(fnr: String, orgnummer: String) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM inntektsmelding WHERE fnr = :fnr and orgnummer = :orgnummer", mapOf("fnr" to fnr, "orgnummer" to orgnummer)).map { row ->
                row.long(1)
            }.asSingle)
        }

    private fun genererSendeklarInntektsmelding(arkivreferanse: String, mottatt: LocalDateTime): SendeklarInntektsmelding {
        return SendeklarInntektsmelding(FØDSELSNUMMER, ORGNUMMER, Melding.Inntektsmelding(genererInntektsmelding(arkivreferanse = arkivreferanse)), mottatt)
    }

    private val registry = SimpleMeterRegistry()
    private fun genererInntektsmelding(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arbeidsforholdId: String? = null, arkivreferanse: String): JsonMessage {
        val arbeidsforholdIdJson = if (arbeidsforholdId == null) "" else """ "arbeidsforholdId": "$arbeidsforholdId", """
        val inntektsmelding = """{
        "arbeidstakerFnr": "$fnr",
        "virksomhetsnummer": "$orgnummer",
        "mottattDato": "${LocalDateTime.now()}",
        $arbeidsforholdIdJson
        "arkivreferanse": "$arkivreferanse",
        "inntektsmeldingId": "${UUID.randomUUID()}"
        }"""

        return JsonMessage(inntektsmelding, MessageProblems(inntektsmelding), registry).also {
            it.interestedIn("arbeidstakerFnr")
            it.interestedIn("virksomhetsnummer")
            it.interestedIn("mottattDato")
            it.interestedIn("arkivreferanse")
            it.interestedIn("arbeidsforholdId")
            it.interestedIn("inntektsmeldingId")
        }
    }

    private fun lagreMelding(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arbeidsforholdId: String? = null, arkivreferanse: String, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime = LocalDateTime.now()) : String{
        val inntektsmeldingDao = InntektsmeldingDao(dataSource)
        val meldingDao = MeldingDao(dataSource)
        val melding = Melding.Inntektsmelding(genererInntektsmelding(fnr, orgnummer, arbeidsforholdId, arkivreferanse))
        meldingDao.leggInn(melding)
        inntektsmeldingDao.leggInn(melding, ønsketPublisert, mottatt = mottatt)
        return melding.duplikatkontroll()
    }

}