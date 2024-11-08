package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import no.nav.helse.rapids_rivers.JsonMessage
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime

class BerikelseTest {

    @Test
    fun `skal putte på fødselsdato og aktørId og dødsdato`() {
        val json = Melding.les("ny_søknad", nySøknad)!!
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
        val json = Melding.les("inntektsmelding", inntektsmelding)!!
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
      "opprettet": "${LocalDateTime.now()}"
    }
""".trimIndent()

private val inntektsmelding = """
    {
      "@event_name": "inntektsmelding",
      "mottattDato": "${LocalDateTime.now()}"
    }
""".trimIndent()