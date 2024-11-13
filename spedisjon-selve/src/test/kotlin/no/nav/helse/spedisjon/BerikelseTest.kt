package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import no.nav.helse.spedisjon.Melding.Inntektsmelding
import no.nav.helse.spedisjon.Melding.NySøknad
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

class BerikelseTest {

    @Test
    fun `skal putte på fødselsdato og aktørId og dødsdato`() {
        val json = NySøknad.lagNySøknad(nySøknad)
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
        val json = Inntektsmelding.lagInntektsmelding(inntektsmelding)
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

    private fun JsonMessage.toJsonNode() =
        toJson().let { jacksonObjectMapper().readTree(it) }
}

private val nySøknad = """
    {
      "@event_name": "ny_søknad",
      "opprettet": "${LocalDateTime.now()}",
      "id": "${UUID.randomUUID()}",
      "sykmeldingId": "${UUID.randomUUID()}"
    }
""".trimIndent()

private val inntektsmelding = """
    {
      "@event_name": "inntektsmelding",
      "mottattDato": "${LocalDateTime.now()}",
      "inntektsmeldingId": "${UUID.randomUUID()}"
    }
""".trimIndent()