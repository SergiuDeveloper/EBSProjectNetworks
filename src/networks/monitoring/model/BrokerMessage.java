package networks.monitoring.model;

public enum BrokerMessage {
    SUBSCRIBER_CONNECTED("SUBSCRIBER_CONNECTED"),
    SUBSCRIBER_DISCONNECTED("SUBSCRIBER_DISCONNECTED"),
    BROKER_CONNECTED("BROKER_CONNECTED"),
    BROKER_DISCONNECTED("BROKER_DISCONNECTED");

    private final String message;

    BrokerMessage(final String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return this.message;
    }
}
