package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class Inntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey(
                "inntektsmeldingId", "arbeidstakerFnr",
                "arbeidstakerAktorId", "virksomhetsnummer",
                "arbeidsgivertype", "beregnetInntekt",
                "endringIRefusjoner", "arbeidsgiverperioder",
                "status", "arkivreferanse", "ferieperioder",
                "foersteFravaersdag", "mottattDato"
            ) }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val inntektsmelding = Melding.Inntektsmelding(packet)
        if (!meldingDao.leggInn(inntektsmelding)) return
        //context.send(inntektsmelding.fødselsnummer(), inntektsmelding.json())
    }
}
