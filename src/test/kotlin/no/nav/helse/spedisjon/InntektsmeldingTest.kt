package no.nav.helse.spedisjon

import kotliquery.queryOf
import kotliquery.sessionOf
import kotliquery.using
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.LocalDateTime
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InntektsmeldingTest : AbstractDatabaseTest() {

    @Test
    fun `tar imot inntektsmelding`(){
        val mediator = InntektsmeldingMediator(MeldingDao(dataSource), InntektsmeldingDao(dataSource))
        val im = Melding.Inntektsmelding(inntektsmeldingJsonMessage)
        mediator.lagreInntektsmelding(im)
        assertEquals(1, antallMeldinger(FØDSELSNUMMER))
        assertEquals(1, antallInntektsmeldinger(FØDSELSNUMMER, ORGNUMMER))
    }

    private fun antallInntektsmeldinger(fnr: String, orgnummer: String) =
        using(sessionOf(dataSource)) {
            it.run(queryOf("SELECT COUNT(1) FROM inntektsmelding WHERE fnr = :fnr and orgnummer = :orgnummer", mapOf("fnr" to fnr, "orgnummer" to orgnummer)).map { row ->
                row.long(1)
            }.asSingle)
        }

    val inntektsmelding = """{
        "arbeidstakerFnr": "$FØDSELSNUMMER",
        "virksomhetsnummer": "$ORGNUMMER",
        "mottattDato": "${LocalDateTime.now()}",
        "arkivreferanse": "${UUID.randomUUID()}"
        }"""

    val inntektsmeldingJsonMessage = JsonMessage(inntektsmelding, MessageProblems(inntektsmelding)).also {
        it.interestedIn("arbeidstakerFnr")
        it.interestedIn("virksomhetsnummer")
        it.interestedIn("mottattDato")
        it.interestedIn("arkivreferanse")
    }
}