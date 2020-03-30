package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.RapidsConnection

internal class TestRapid : RapidsConnection() {
    private val context = TestContext()
    private val messages = mutableListOf<Pair<String?, String>>()

    internal fun reset() {
        listeners.clear()
        messages.clear()
    }

    fun sendTestMessage(message: String) {
        listeners.forEach { it.onMessage(message, context) }
    }

    override fun publish(message: String) {
        messages.add(null to message)
    }

    override fun publish(key: String, message: String) {
        messages.add(key to message)
    }

    override fun start() {}

    override fun stop() {}

    private inner class TestContext : MessageContext {
        override fun send(message: String) {
            publish(message)
        }

        override fun send(key: String, message: String) {
            publish(key, message)
        }
    }
}
