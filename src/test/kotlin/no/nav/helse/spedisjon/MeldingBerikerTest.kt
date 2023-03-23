package no.nav.helse.spedisjon

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime

class MeldingBerikerTest {

    @Test
    fun `skal putte på fødselsdato og aktørId og dødsdato`() {
        val json = Melding.les("ny_søknad", nySøknad)!!
        val beriketJson = PersonBerikerMediator.berik(
            json,
            LocalDate.of(2012, 12, 31),
            "12345",
            LocalDate.of(2023, 1, 2)
        )
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertEquals("2023-01-02", beriketJson.path("dødsdato").asText())
        assertEquals("12345", beriketJson.path("aktorId").asText())
        assertEquals("ny_søknad", beriketJson.path("@event_name").asText())
    }

    @Test
    fun `inntektsmelding har rart aktørid-feltnavn`() {
        val json = Melding.les("inntektsmelding", inntektsmelding)!!
        val beriketJson = PersonBerikerMediator.berik(
            json,
            LocalDate.of(2012, 12, 31),
            "12345"
        )
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertEquals("12345", beriketJson.path("arbeidstakerAktorId").asText())
        assertEquals("inntektsmelding", beriketJson.path("@event_name").asText())
    }
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