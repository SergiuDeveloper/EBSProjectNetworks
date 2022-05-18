package networks.monitoring.model;

public enum MonitoringClientType {
    BROKER("BROKER"),
    SUBSCRIBER("SUBSCRIBER");

    private final String type;

    MonitoringClientType(final String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return this.type;
    }
}
