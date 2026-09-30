package no.nav.helse.spedisjon.api.tjeneste

import java.util.*
import no.nav.helse.spedisjon.api.MeldingDao
import no.nav.helse.spedisjon.api.MeldingDto
import no.nav.helse.spedisjon.api.NyMeldingDto

internal class ApiMeldingtjeneste(private val dao: MeldingDao) {
    fun lagreNyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val dto = NyMeldingDto(
            type = request.type,
            fnr = request.fnr,
            eksternDokumentId = request.eksternDokumentId,
            duplikatkontroll = request.duplikatkontroll,
            jsonBody = request.jsonBody
        )
        val result = dao.leggInn(dto)
        return NyMeldingResponse(
            internDokumentId = result.internId,
            bleLagtInnNå = result.utfall == MeldingDao.Resultat.Utfall.SATT_INN_NY
        )
    }

    fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        return HentMeldingerResponse(
            meldinger = dao.hentMeldinger(interneDokumentIder)
        )
    }
}

data class NyMeldingResponse(val internDokumentId: UUID, val bleLagtInnNå: Boolean)

data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val duplikatkontroll: String,
    val jsonBody: String
)

class HentMeldingerResponse(
    val meldinger: List<MeldingDto>
)
