package nostr.postr

import nostr.postr.events.Event
import java.util.UUID

/**
 * The Nostr Client manages multiple personae the user may switch between. Events are received and
 * published through multiple relays.
 * Events are stored with their respective persona.
 */
object Client: RelayPool.Listener {
    /**
     * Lenient mode:
     *
     * true: For maximum compatibility. If you want to play ball with sloppy counterparts, use
     *       this.
     * false: For developers who want to make protocol compliant counterparts. If your software
     *        produces events that fail to deserialize in strict mode, you should probably fix
     *        something.
     **/
    var lenient: Boolean = false
    val personae: Array<Persona> = emptyArray()
    private val listeners = HashSet<Listener>()
    internal val subscriptions: MutableMap<String, MutableList<JsonFilter>> = mutableMapOf()

    fun send(signedEvent: Event) {
        RelayPool.send(signedEvent)
    }

    fun close(subscriptionId: String){
        RelayPool.close(subscriptionId)
    }

    fun disconnect() {
        RelayPool.unregister(this)
        RelayPool.disconnect()
    }

    override fun onEvent(event: Event, subscriptionId: String, relay: Relay) {
        listeners.forEach { it.onEvent(event, subscriptionId, relay) }
    }

    override fun onNewEvent(event: Event, subscriptionId: String) {
        listeners.forEach { it.onNewEvent(event, subscriptionId) }
    }

    override fun onError(error: Error, subscriptionId: String, relay: Relay) {
        listeners.forEach { it.onError(error, subscriptionId, relay) }
    }

    override fun onRelayStateChange(type: Relay.Type, relay: Relay) {
        listeners.forEach { it.onRelayStateChange(type, relay) }
    }

    fun subscribe(listener: Listener) {
        listeners.add(listener)
    }

    fun unsubscribe(listener: Listener): Boolean {
        return listeners.remove(listener)
    }


    abstract class Listener {
        /**
         * A new message was received
         */
        open fun onEvent(event: Event, subscriptionId: String, relay: Relay) = Unit

        /**
         * A new message was received
         */
        open fun onNewEvent(event: Event, subscriptionId: String) = Unit

        /**
         * A new or repeat message was received
         */

        open fun onError(error: Error, subscriptionId: String, relay: Relay) = Unit

        /**
         * Connected to or disconnected from a relay
         */
        open fun onRelayStateChange(type: Relay.Type, relay: Relay) = Unit
    }
}