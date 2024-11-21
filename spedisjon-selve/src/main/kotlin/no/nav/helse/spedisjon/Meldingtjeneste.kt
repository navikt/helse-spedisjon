package no.nav.helse.spedisjon

import java.time.LocalDateTime
import java.util.UUID

interface Meldingtjeneste {
    fun nyMelding(meldingsdetaljer: NyMeldingRequest): NyMeldingResponse
    fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse
}

internal class LokalMeldingtjeneste(private val dao: MeldingDao) : Meldingtjeneste {
    override fun nyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val dto = NyMeldingDto(
            type = request.type,
            fnr = request.fnr,
            eksternDokumentId = request.eksternDokumentId,
            rapportertDato = request.rapportertDato,
            duplikatkontroll = request.duplikatkontroll,
            jsonBody = request.jsonBody
        )
        val internId = dao.leggInn(dto) ?: return NyMeldingResponse.Duplikatkontroll
        return NyMeldingResponse.OK(internId)
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        return HentMeldingerResponse(
            meldinger = dao.hentMeldinger(interneDokumentIder)
        )
    }
}

sealed interface NyMeldingResponse {
    data object Duplikatkontroll : NyMeldingResponse
    data class OK(val internDokumentId: UUID) : NyMeldingResponse
}

data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)

class HentMeldingerResponse(
    val meldinger: List<MeldingDto>
)