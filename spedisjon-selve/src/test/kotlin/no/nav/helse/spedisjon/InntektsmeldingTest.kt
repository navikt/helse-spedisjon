package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import io.mockk.mockk
import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.spedisjon.Meldingsdetaljer.Companion.sha512
import no.nav.helse.spedisjon.SendeklarInntektsmelding.Companion.sorter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.time.LocalDateTime
import java.util.*

class InntektsmeldingTest : AbstractDatabaseTest() {

    private lateinit var meldingMediator: MeldingMediator
    private lateinit var mediator: InntektsmeldingMediator
    private lateinit var inntektsmeldingDao: InntektsmeldingDao

    @BeforeEach
    fun before() {
        val meldingtjeneste = LokalMeldingtjeneste(MeldingDao(dataSource))
        inntektsmeldingDao = InntektsmeldingDao(meldingtjeneste, dataSource)
        meldingMediator = MeldingMediator(meldingtjeneste, mockk(), mockk(relaxed = true))
        mediator = InntektsmeldingMediator(mockk(), inntektsmeldingDao, dokumentAliasProducer = mockk(relaxed = true))

    }

    @Test
    fun `tar imot inntektsmelding`() {
        val detaljer = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = FØDSELSNUMMER,
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf("a"),
            jsonBody = """{ "mottattDato": "${LocalDateTime.now()}" }"""
        )

        val internId = meldingMediator.leggInnMelding(detaljer) ?: fail { "skulle legge inn melding" }
        val im = Melding.Inntektsmelding(internId, ORGNUMMER, null, detaljer)
        mediator.lagreInntektsmelding(im, TestRapid())

        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    @Test
    fun `lagrer inntektsmelding bare en gang`() {
        val detaljer = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = FØDSELSNUMMER,
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf("a"),
            jsonBody = """{ "mottattDato": "${LocalDateTime.now()}" }"""
        )

        val internId = meldingMediator.leggInnMelding(detaljer) ?: fail { "skulle legge inn melding" }
        val im = Melding.Inntektsmelding(internId, ORGNUMMER, null, detaljer)
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
        val metaInntektsmeldinger = buildList {
            inntektsmeldingDao.hentSendeklareMeldinger(0) { im, _ ->
                add(im)
            }
        }
        assertEquals(2, metaInntektsmeldinger.size)
        assertEquals(b, metaInntektsmeldinger.first().melding.meldingsdetaljer.duplikatkontroll)
    }

    @Test
    fun `teller flere inntektsmeldinger`() {
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
        buildList {
            inntektsmeldingDao.hentSendeklareMeldinger(timeout) { im, antall ->
                add(im to antall)
            }
        }

    @Test
    fun `sendeklar inntektsmelding sorteringstest`() {
        val sendeKlareInntektsmeldinger = listOf(
            genererSendeklarInntektsmelding("andre", LocalDateTime.now()),
            genererSendeklarInntektsmelding("første", LocalDateTime.now().minusMinutes(1)),
            genererSendeklarInntektsmelding("tredje", LocalDateTime.now().plusMinutes(1))
        ).sorter()

        assertEquals(listOf("første", "andre", "tredje").map { it.sha512() }, sendeKlareInntektsmeldinger.map { it.melding.meldingsdetaljer.duplikatkontroll })
    }

    private fun antallInntektsmeldinger(fnr: String, orgnummer: String) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM inntektsmelding WHERE fnr = :fnr and orgnummer = :orgnummer", mapOf("fnr" to fnr, "orgnummer" to orgnummer)).map { row ->
                row.long(1)
            }.asSingle)
        }

    private fun genererSendeklarInntektsmelding(arkivreferanse: String, mottatt: LocalDateTime): SendeklarInntektsmelding {
        val detaljer = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = FØDSELSNUMMER,
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf(arkivreferanse),
            jsonBody = """{ "mottattDato": "${LocalDateTime.now()}" }"""
        )

        val im = Melding.Inntektsmelding(UUID.randomUUID(), ORGNUMMER, null, detaljer)
        return SendeklarInntektsmelding(FØDSELSNUMMER, ORGNUMMER, im, mottatt)
    }

    private fun lagreMelding(fnr: String = FØDSELSNUMMER, orgnummer: String = ORGNUMMER, arbeidsforholdId: String? = null, arkivreferanse: String, ønsketPublisert: LocalDateTime, mottatt: LocalDateTime = LocalDateTime.now()) : String{
        val detaljer = Meldingsdetaljer(
            type = "inntektsmelding",
            fnr = fnr,
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf(arkivreferanse),
            jsonBody = """{ "mottattDato": "${LocalDateTime.now()}" }"""
        )

        val internId = meldingMediator.leggInnMelding(detaljer) ?: fail { "skulle legge inn melding" }
        val melding = Melding.Inntektsmelding(internId, orgnummer, arbeidsforholdId, detaljer)
        mediator.lagreInntektsmelding(melding, TestRapid(), ønsketPublisert = ønsketPublisert, mottatt = mottatt)
        return melding.meldingsdetaljer.duplikatkontroll
    }

}