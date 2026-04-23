package no.nav.helse.spedisjon.api

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.ktor.http.*
import io.ktor.server.plugins.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.util.*
import java.util.*
import no.nav.helse.spedisjon.api.tjeneste.Meldingtjeneste

internal fun Route.api(meldingtjeneste: Meldingtjeneste) {
    route("/api/melding") {
        /*
            sette inn ny melding i db

            409 Conflict hvis meldingen allerede finnes
            200 OK hvis OK
         */
        post {
            val request = call.receive<NyMeldingRequest>()
            val dto = no.nav.helse.spedisjon.api.tjeneste.NyMeldingRequest(
                type = request.type,
                fnr = request.fnr,
                eksternDokumentId = request.eksternDokumentId,
                duplikatkontroll = request.duplikatkontroll,
                jsonBody = request.jsonBody
            )
            val response = meldingtjeneste.nyMelding(dto)

            call.respond(if (response.lagtInnNå) HttpStatusCode.OK else HttpStatusCode.Conflict, NyMeldingResponse(
                internDokumentId = response.internDokumentId
            ))
        }
        /* hente melding */
        get("/{internDokumentId}") {
            val internDokumentId = UUID.fromString(call.parameters.getOrFail("internDokumentId"))
            val response = meldingtjeneste.hentMeldinger(listOf(internDokumentId))

            if (response.meldinger.size != 1) throw NotFoundException()
            val melding = response.meldinger.single()
            call.respond(HttpStatusCode.OK, MeldingResponse(
                type = melding.type,
                fnr = melding.fnr,
                internDokumentId = melding.internDokumentId,
                eksternDokumentId = melding.eksternDokumentId,
                duplikatkontroll = melding.duplikatkontroll,
                jsonBody = melding.jsonBody
            ))
        }
    }
    /* hente meldinger (flertall) */
    get("/api/meldinger") {
        val request = call.receive<HentMeldingerRequest>()
        val response = meldingtjeneste.hentMeldinger(request.internDokumentIder)

        val meldinger = response.meldinger.map { melding ->
            MeldingResponse(
                type = melding.type,
                fnr = melding.fnr,
                internDokumentId = melding.internDokumentId,
                eksternDokumentId = melding.eksternDokumentId,
                duplikatkontroll = melding.duplikatkontroll,
                jsonBody = melding.jsonBody
            )
        }
        call.respond(HttpStatusCode.OK, HentMeldingerResponse(meldinger = meldinger))
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)

data class NyMeldingResponse(
    val internDokumentId: UUID
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class HentMeldingerRequest(
    val internDokumentIder: List<UUID>
)

data class HentMeldingerResponse(
    val meldinger: List<MeldingResponse>
)

data class MeldingResponse(
    val type: String,
    val fnr: String,
    val internDokumentId: UUID,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)
