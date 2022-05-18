package networks.monitoring;

import networks.monitoring.model.BrokerInfo;
import networks.monitoring.model.BrokerMessage;
import networks.monitoring.model.MonitoringClientType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class MonitoringClient {

    private final MonitoringClientType monitoringClientType;

    private Socket clientSocket;
    private BufferedReader in;
    private PrintWriter out;

    public MonitoringClient(MonitoringClientType monitoringClientType) {
        this.monitoringClientType = monitoringClientType;
    }

    public void connectBroker(String ip, int port, int brokerSubscriptionsServerPort, int brokerBrokersServerPort, Function<BrokerInfo, Void> brokerConnectedCallback, Function<BrokerInfo, Void> brokerDisconnectedCallback) throws IOException {
        if (this.monitoringClientType != MonitoringClientType.BROKER) {
            throw new RuntimeException("Monitoring client is not broker");
        }

        this.clientSocket = new Socket(ip, port);
        this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        this.out = new PrintWriter(clientSocket.getOutputStream(), true);

        this.brokerLogic(brokerSubscriptionsServerPort, brokerBrokersServerPort, brokerConnectedCallback, brokerDisconnectedCallback);
    }

    public Map<BrokerInfo, Integer> connectSubscriber(String ip, int port, int newSubscriptionsCount) throws IOException {
        if (this.monitoringClientType != MonitoringClientType.SUBSCRIBER) {
            throw new RuntimeException("Monitoring client is not subscriber");
        }

        this.clientSocket = new Socket(ip, port);
        this.in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        this.out = new PrintWriter(clientSocket.getOutputStream(), true);

        return this.subscriberLogic(newSubscriptionsCount);
    }

    public void sendFeedSubscriptionsIncrementMessage(int subscriptionsAdded) {
        if (this.monitoringClientType != MonitoringClientType.BROKER) {
            throw new RuntimeException("Monitoring client is not broker");
        }

        this.out.println(BrokerMessage.SUBSCRIBER_CONNECTED);
        this.out.println(subscriptionsAdded);
    }

    public void sendFeedSubscriptionsDecrementMessage(int subscriptionsAdded) {
        if (this.monitoringClientType != MonitoringClientType.BROKER) {
            throw new RuntimeException("Monitoring client is not broker");
        }

        this.out.println(BrokerMessage.SUBSCRIBER_DISCONNECTED);
        this.out.println(subscriptionsAdded);
    }

    private void brokerLogic(int brokerSubscriptionsServerPort, int brokerBrokersServerPort, Function<BrokerInfo, Void> brokerConnectedCallback, Function<BrokerInfo, Void> brokerDisconnectedCallback) {
        this.out.println(brokerSubscriptionsServerPort);
        this.out.println(brokerBrokersServerPort);

        Thread receiveMonitoringUpdatesThread = new Thread(() -> {
            while (true) {
                try {
                    BrokerMessage brokerMessage = BrokerMessage.valueOf(this.in.readLine());
                    String externalBrokerIp = this.in.readLine();
                    int externalBrokerBrokersServerPort = Integer.parseInt(this.in.readLine());
                    BrokerInfo externalBroker = new BrokerInfo(externalBrokerIp, -1, externalBrokerBrokersServerPort);

                    switch (brokerMessage) {
                        case BROKER_CONNECTED: brokerConnectedCallback.apply(externalBroker); break;
                        case BROKER_DISCONNECTED: brokerDisconnectedCallback.apply(externalBroker); break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        receiveMonitoringUpdatesThread.start();
    }

    private Map<BrokerInfo, Integer> subscriberLogic(int newSubscriptionsCount) throws IOException {
        this.out.println(newSubscriptionsCount);

        Map<BrokerInfo, Integer> subscriptionsToAddPerBroker = new HashMap<>();

        while (true) {
            String brokerIp = this.in.readLine();
            if (Objects.equals(brokerIp, "Done")) {
                break;
            }
            int subscriptionsServerPort = Integer.parseInt(this.in.readLine());
            int subscriptionsToAdd = Integer.parseInt(this.in.readLine());

            BrokerInfo brokerInfo = new BrokerInfo(brokerIp, subscriptionsServerPort, -1);
            subscriptionsToAddPerBroker.put(brokerInfo, subscriptionsToAdd);
        }
        this.out.println("Done");
        try {
            this.clientSocket.close();
        } catch (IOException ignored) {}

        return subscriptionsToAddPerBroker;
    }
}
