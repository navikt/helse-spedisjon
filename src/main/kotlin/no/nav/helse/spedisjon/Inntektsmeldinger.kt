package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class Inntektsmeldinger(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao,
    private val problemsCollector: ProblemsCollector,
    private val aktørregisteretClient: AktørregisteretClient
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate {
                it.requireKey(
                    "inntektsmeldingId",
                    "arbeidstakerAktorId", "virksomhetsnummer",
                    "arbeidsgivertype", "beregnetInntekt",
                    "endringIRefusjoner", "arbeidsgiverperioder",
                    "status", "arkivreferanse", "ferieperioder",
                    "foersteFravaersdag", "mottattDato"
                )
            }
            validate { it.interestedIn("arbeidstakerFnr") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet.putIfAbsent("arbeidstakerFnr") { aktørregisteretClient.hentFødselsnummer(packet["arbeidstakerAktorId"].asText()) }
        val inntektsmelding = Melding.Inntektsmelding(packet)
        if (!meldingDao.leggInn(inntektsmelding)) return
        context.send(inntektsmelding.fødselsnummer(), inntektsmelding.json())
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        problemsCollector.add("Inntektsmelding", problems)
    }
}
