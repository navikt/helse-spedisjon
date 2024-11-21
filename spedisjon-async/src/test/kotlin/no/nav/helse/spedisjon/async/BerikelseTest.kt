package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.spedisjon.async.Melding.Inntektsmelding
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

class BerikelseTest {

    @Test
    fun `skal putte på fødselsdato og aktørId og dødsdato`() {
        val json = Melding.NySøknad(UUID.randomUUID(), Meldingsdetaljer(
            type = "ny_søknad",
            fnr = "fnr",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf("unik key"),
            jsonBody = "{}"
        ))
        val beriketJson = Berikelse(
            fødselsdato = LocalDate.of(2012, 12, 31),
            dødsdato = LocalDate.of(2023, 1, 2),
            aktørId = "12345",
            historiskeFolkeregisteridenter = emptyList()
        ).berik(json).toJsonNode()
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertEquals("2023-01-02", beriketJson.path("dødsdato").asText())
        assertEquals("12345", beriketJson.path("aktorId").asText())
        assertEquals("ny_søknad", beriketJson.path("@event_name").asText())
    }

    @Test
    fun `inntektsmelding blir ikke beriket med aktørid`() {
        val json = Inntektsmelding(
            internId = UUID.randomUUID(),
            orgnummer = "orgnr",
            arbeidsforholdId = null,
            meldingsdetaljer = Meldingsdetaljer(
                type = "inntektsmelding",
                fnr = "fnr",
                eksternDokumentId = UUID.randomUUID(),
                rapportertDato = LocalDateTime.now(),
                duplikatnøkkel = listOf("unik key"),
                jsonBody = "{}"
            )
        )
        val beriketJson = Berikelse(
            fødselsdato = LocalDate.of(2012, 12, 31),
            dødsdato = null,
            aktørId = "12345",
            historiskeFolkeregisteridenter = emptyList()
        ).berik(json).toJsonNode()
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertTrue(beriketJson.path("arbeidstakerAktorId").isMissingNode)
        assertEquals("inntektsmelding", beriketJson.path("@event_name").asText())
    }

    private fun BeriketMelding.toJsonNode() =
        jacksonObjectMapper().readTree(json)
}
