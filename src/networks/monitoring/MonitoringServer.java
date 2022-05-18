package networks.monitoring;

import networks.monitoring.model.BrokerInfo;
import networks.monitoring.model.BrokerMessage;
import networks.monitoring.model.MonitoringClientType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MonitoringServer {

    private boolean running;
    private final Object runningLock;

    private final Map<BrokerInfo, Integer> brokerSubscriptionsMap;
    private final Object brokerSubscriptionsMapLock;

    private int subscriptionsCount;
    private final Object subscriptionsCountLock;

    public MonitoringServer() {
        this.running = false;
        this.runningLock = new Object();

        this.brokerSubscriptionsMap = new HashMap<>();
        this.brokerSubscriptionsMapLock = new Object();

        this.subscriptionsCount = 0;
        this.subscriptionsCountLock = new Object();
    }

    public void start(int port) throws IOException {
        if (this.isRunning()) {
            throw new RuntimeException("Monitoring server already running");
        }

        ServerSocket serverSocket = new ServerSocket(port);
        this.setRunning(true);

        System.out.printf("Monitoring server running on port %d%n", port);

        while (this.isRunning()) {
            try {
                Socket clientSocket = serverSocket.accept();
                Thread clientHandlingLogicThread = new Thread(() -> {
                    try {
                        this.clientHandlingLogic(clientSocket);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                clientHandlingLogicThread.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop() {
        if (!this.isRunning()) {
            throw new RuntimeException("Monitoring server not running");
        }
        this.setRunning(false);
    }

    public boolean isRunning() {
        synchronized (this.runningLock) {
            return this.running;
        }
    }

    public void setRunning(boolean running) {
        synchronized (this.runningLock) {
            this.running = running;
        }
    }

    private int getSubscriptionsCount() {
        synchronized (this.subscriptionsCountLock) {
            return this.subscriptionsCount;
        }
    }

    private void addBrokerToConnectionsMap(BrokerInfo brokerInfo) {
        synchronized (this.brokerSubscriptionsMapLock) {
            if (this.brokerSubscriptionsMap.containsKey(brokerInfo)) {
                throw new RuntimeException("Broker already exists");
            }
            this.brokerSubscriptionsMap.put(brokerInfo, 0);
        }
    }

    private void removeBrokerFromConnectionsMap(BrokerInfo brokerInfo) {
        synchronized (this.brokerSubscriptionsMapLock) {
            int previousConnectionsCount = this.brokerSubscriptionsMap.get(brokerInfo);
            this.brokerSubscriptionsMap.remove(brokerInfo);

            synchronized (this.subscriptionsCountLock) {
                this.subscriptionsCount -= subscriptionsCount;
            }
        }
    }

    private void incrementBrokerSubscriptionsMap(BrokerInfo brokerInfo, int subscriptionsCount) {
        synchronized (this.brokerSubscriptionsMapLock) {
            int previousConnectionsCount = this.brokerSubscriptionsMap.get(brokerInfo);
            this.brokerSubscriptionsMap.put(brokerInfo, previousConnectionsCount + subscriptionsCount);

            synchronized (this.subscriptionsCountLock) {
                this.subscriptionsCount += subscriptionsCount;
            }
        }
    }

    private void decrementBrokerSubscriptionsMap(BrokerInfo brokerInfo, int subscriptionsCount) {
        synchronized (this.brokerSubscriptionsMapLock) {
            int previousConnectionsCount = this.brokerSubscriptionsMap.get(brokerInfo);
            this.brokerSubscriptionsMap.put(brokerInfo, previousConnectionsCount - subscriptionsCount);

            synchronized (this.subscriptionsCountLock) {
                this.subscriptionsCount -= subscriptionsCount;
            }
        }
    }

    private void clientHandlingLogic(Socket clientSocket) throws IOException {
        BufferedReader in;
        PrintWriter out;

        MonitoringClientType monitoringClientType;
        try {
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            monitoringClientType = MonitoringClientType.valueOf(in.readLine());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        switch (monitoringClientType) {
            case BROKER: this.handleBrokerClient(clientSocket, in); break;
            case SUBSCRIBER: this.handleSubscriberClient(clientSocket, in, out); break;
        }
    }

    private void handleBrokerClient(Socket clientSocket, BufferedReader in) throws IOException {
        // Add broker to brokers map
        String ip = ((InetSocketAddress)clientSocket.getRemoteSocketAddress()).getAddress().toString();
        int port;
        try {
            port = Integer.parseInt(in.readLine());
        } catch (IOException e) {
            clientSocket.close();
            System.out.println("Broker disconnected from monitoring server");
            return;
        }
        BrokerInfo brokerInfo = new BrokerInfo(ip, port);
        try {
            this.addBrokerToConnectionsMap(brokerInfo);
        } catch (RuntimeException e) {
            e.printStackTrace();
            clientSocket.close();
            System.out.println("Failed to add broker to brokers map, on monitoring server");
            return;
        }
        System.out.println("Broker connected to monitoring server");

        // Get messages from broker regarding subscriber connections
        while (true) {
            BrokerMessage brokerMessage;
            try {
                brokerMessage = BrokerMessage.valueOf(in.readLine());
            } catch (Exception e) {
                clientSocket.close();
                this.removeBrokerFromConnectionsMap(brokerInfo);
                System.out.println("Broker disconnected from monitoring server");
                return;
            }

            int subscriptionsCount;
            try {
                subscriptionsCount = Integer.parseInt(in.readLine());
            } catch (Exception e) {
                clientSocket.close();
                this.removeBrokerFromConnectionsMap(brokerInfo);
                System.out.println("Broker disconnected from monitoring server");
                return;
            }

            switch (brokerMessage) {
                case SUBSCRIBER_CONNECTED: this.incrementBrokerSubscriptionsMap(brokerInfo, subscriptionsCount); break;
                case SUBSCRIBER_DISCONNECTED: this.decrementBrokerSubscriptionsMap(brokerInfo, subscriptionsCount); break;
            }
        }
    }

    private void handleSubscriberClient(Socket clientSocket, BufferedReader in, PrintWriter out) throws IOException {
        int newSubscriptionsCount;
        try {
            newSubscriptionsCount = Integer.parseInt(in.readLine());
        } catch (Exception e) {
            clientSocket.close();
            System.out.println("Subscriber disconnected from monitoring server");
            return;
        }

        // Computer number of subscriptions to add per broker
        int totalSubscriptionsCount = this.getSubscriptionsCount();
        int totalNewSubscriptionsCount = newSubscriptionsCount + totalSubscriptionsCount;

        Map<BrokerInfo, Integer> subscriptionsToAddPerBroker = new HashMap<>();
        BrokerInfo lastBroker = null;
        int lastBrokerExtraSubscriptions;
        synchronized (this.brokerSubscriptionsMapLock) {
            Set<BrokerInfo> brokerInfoSet = this.brokerSubscriptionsMap.keySet();
            int brokersCount = brokerInfoSet.size();
            int meanSubscriptionsPerBroker = totalNewSubscriptionsCount / brokersCount + 1;
            lastBrokerExtraSubscriptions = meanSubscriptionsPerBroker * brokersCount - totalNewSubscriptionsCount;

            for (BrokerInfo brokerInfo: brokerInfoSet) {
                lastBroker = brokerInfo;
                subscriptionsToAddPerBroker.put(brokerInfo, meanSubscriptionsPerBroker - this.brokerSubscriptionsMap.get(brokerInfo));
            }
        }
        if (lastBroker == null) {
            clientSocket.close();
            System.out.println("No brokers available");
            return;
        }
        subscriptionsToAddPerBroker.put(lastBroker, subscriptionsToAddPerBroker.get(lastBroker) - lastBrokerExtraSubscriptions);

        // Send number of subscriptions to add per broker, to the new subscriber
        for (BrokerInfo brokerInfo: subscriptionsToAddPerBroker.keySet()) {
            int subscriptionsToAdd = subscriptionsToAddPerBroker.get(brokerInfo);
            out.println(brokerInfo.getIp());
            out.println(brokerInfo.getPort());
            out.println(subscriptionsToAdd);
        }
        // Send acknowledgement to subscriber
        out.println("Done");
        System.out.println("Sent number of subscriptions to add per broker, to the new subscriber");

        // Await acknowledgement from subscriber
        in.readLine();
        clientSocket.close();
    }
}
