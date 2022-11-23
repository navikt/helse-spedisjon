package no.nav.helse.spedisjon

import com.fasterxml.jackson.databind.JsonNode

internal class SendeklarInntektsmelding(
    private val fnr: String,
    private val orgnummer: String,
    val originalMelding: Melding.Inntektsmelding,
    private val berikelse: JsonNode
) {

}