package nostr.postr.examples

import nostr.postr.Client
import nostr.postr.events.Event
import nostr.postr.events.MetadataEvent
import nostr.postr.toHex

/**
 * Get a user's meta data
 */
class LoadOneUserProfile {
    companion object {
        private val pubKey = "46fcbe3065eaf1ae7811465924e48923363ff3f526bd6f73d7c184b16bd8ce4d"
        private val listener = object: Client.Listener() {
            override fun onNewEvent(event: Event) {
                if (event.pubKey.toHex() == pubKey) {
                    (event as? MetadataEvent)?.contactMetaData?.run {
                        logDetail(
                            event,
                            "received name: ${name.trim()}, about: ${about.trim()}, nip05: ${nip05?.trim()}, picture: ${picture.trim()}"
                        )
                        stop()
                    }
                } else {
                    logDetail(event, "Why do we get this event? ${event.id}")
                }
            }
        }

        @JvmStatic
        fun main(vararg args: String) {
            Client.subscribe(listener)
            val filters = mutableListOf("""{"kinds":[${MetadataEvent.kind}],"authors":["$pubKey"]}""")
            Client.connect(filters)
        }

        private fun stop() {
            Client.unsubscribe(listener)
            Client.disconnect()
        }
    }
}