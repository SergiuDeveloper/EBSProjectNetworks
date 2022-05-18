package networks.monitoring.model;

import java.util.Objects;

public class BrokerInfo {
    private final String ip;
    private final int subscribersServerPort;
    private final int brokersServerPort;

    public BrokerInfo(String ip, int subscribersServerPort, int brokersServerPort) {
        this.ip = ip;
        this.subscribersServerPort = subscribersServerPort;
        this.brokersServerPort = brokersServerPort;
    }

    public String getIp() {
        return this.ip;
    }

    public int getSubscribersServerPort() {
        return this.subscribersServerPort;
    }

    public int getBrokersServerPort() {
        return this.brokersServerPort;
    }

    @Override
    public String toString() {
        return "BrokerInfo{" +
                "ip='" + ip + '\'' +
                ", subscribersServerPort=" + subscribersServerPort +
                ", brokersServerPort=" + brokersServerPort +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerInfo that = (BrokerInfo) o;
        return subscribersServerPort == that.subscribersServerPort && brokersServerPort == that.brokersServerPort && ip.equals(that.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, subscribersServerPort, brokersServerPort);
    }
}
