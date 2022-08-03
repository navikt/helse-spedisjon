package no.nav.helse.spedisjon

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.LocalDate

class MeldingBerikerTest {

    @Test
    fun `skal putte på fødselsdato og aktørId`() {
        val json = jacksonObjectMapper().readTree(nySøknad)
        val beriketJson = MeldingMediator.berik(
            "01010112345" to json,
            LocalDate.of(2012, 12, 31),
            "12345"
        )
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertEquals("12345", beriketJson.path("aktorId").asText())
        assertEquals("ny_søknad", beriketJson.path("@event_name").asText())
    }

    @Test
    fun `inntektsmelding har rart aktørid-feltnavn`() {
        val json = jacksonObjectMapper().readTree(inntektsmelding)
        val beriketJson = MeldingMediator.berik(
            "01010112345" to json,
            LocalDate.of(2012, 12, 31),
            "12345"
        )
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertEquals("12345", beriketJson.path("arbeidstakerAktorId").asText())
        assertEquals("inntektsmelding_beriket", beriketJson.path("@event_name").asText())
    }
}

private val nySøknad = """
    {
      "@event_name": "ny_søknad"
    }
""".trimIndent()

private val inntektsmelding = """
    {
      "@event_name": "inntektsmelding"
    }
""".trimIndent()