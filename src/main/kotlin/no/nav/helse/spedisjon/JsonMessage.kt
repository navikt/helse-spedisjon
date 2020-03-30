package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.isMissingOrNull

internal fun JsonMessage.putIfAbsent(key: String, block: () -> String?) {
    if (!this[key].isMissingOrNull()) return
    block()?.also { this[key] = it }
}
