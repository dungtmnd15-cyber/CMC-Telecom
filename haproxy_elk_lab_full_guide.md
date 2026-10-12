# TÀI LIỆU HƯỚNG DẪN DỰNG HỆ THỐNG GIÁM SÁT KẾT HỢP LOG HAPROXY VỚI MÔ HÌNH ELK STACK & KAFKA (TỪ ĐẦU)

Tài liệu tổng hợp chi tiết toàn bộ quy trình chuẩn bị môi trường, cài đặt phần mềm hệ thống, cấu hình pipeline và trực quan hóa dữ liệu log HAProxy tập trung.

## I. MÔ HÌNH TỔNG QUAN VÀ PHÂN BỔ HẠ TẦNG

### 1. Kiến trúc luồng dữ liệu (Log Pipeline)

$$
\text{HAProxy Logs} \xrightarrow{\text{UDP 514}} \text{rsyslog (/var/log/haproxy.log)} \xrightarrow{\text{Filebeat}} \text{Kafka (Topic: haproxy-logs)} \xrightarrow{\text{Logstash (Grok)}} \text{Elasticsearch} \xrightarrow{\text{Kibana}}
$$

* **HAProxy**: Đẩy HTTP log truy cập qua giao thức Syslog UDP 514 local.

* **rsyslog**: Lắng nghe UDP 514 và ghi dữ liệu thô vào file `/var/log/haproxy.log`.

* **Filebeat**: Theo dõi file log, đóng gói log dưới dạng JSON và chuyển đến Apache Kafka.

* **Kafka**: Message Broker trung gian hỗ trợ đệm dữ liệu tránh quá tải hệ thống.

* **Logstash**: Đọc log từ Kafka topic `haproxy-logs`, decode JSON, bóc tách cấu trúc log chi tiết bằng Grok Pattern và đẩy dữ liệu tới Elasticsearch.

* **Elasticsearch**: Lưu trữ dữ liệu log đã bóc tách, hỗ trợ truy vấn và tìm kiếm thời gian thực.

* **Kibana**: Giao diện trực quan hóa dữ liệu log thông qua Data View và Dashboard.

### 2. Phân bổ hạ tầng & Địa chỉ IP

| **Máy chủ (Node)** | **Tên Host** | **Địa chỉ IP** | **Thành phần dịch vụ triển khai** | 
| **VM 1** | `student08-server` | `192.168.153.120` | `Docker Engine`, `rsyslog`, `Filebeat`, `HAProxy`, `Kafka`, `Zookeeper`, `Logstash` | 
| **VM 2** | `dung-client` | `192.168.153.105` | `Docker Engine`, `Elasticsearch` (1 Node - `es01`), `Kibana` (`kibana01`) | 

## II. BƯỚC 1: CÀI ĐẶT MÔI TRƯỜNG NỀN TẢNG (Thực hiện trên CẢ 2 VM)

### 1. Cập nhật hệ thống và cài gói phụ trợ

```
sudo apt update && sudo apt upgrade -y
sudo apt install -y curl wget apt-transport-https ca-certificates gnupg lsb-release net-tools

```

### 2. Cài đặt Docker Engine & Docker Compose Plugin

```
# Thêm GPG Key chính thức của Docker
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Thêm Docker Repository
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Cài đặt Docker Engine & Compose
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Khởi chạy dịch vụ systemd và phân quyền user
sudo systemctl enable --now docker
sudo usermod -aG docker $USER

```

## III. BƯỚC 2: CÀI ĐẶT VÀ CẤU HÌNH TRÊN VM 1 (`student08-server` - 192.168.153.120)

### 1. Cài đặt Filebeat trên Host qua Systemd

```
# Thêm Repository Elastic
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-8.x.list

# Cài đặt và bật dịch vụ
sudo apt update && sudo apt install -y filebeat
sudo systemctl enable filebeat

```

### 2. Cấu hình `rsyslog` lắng nghe UDP 514

```
# Tạo file log và chown cho syslog
sudo touch /var/log/haproxy.log
sudo chown syslog:adm /var/log/haproxy.log
sudo chmod 644 /var/log/haproxy.log

# Cấu hình rsyslog
sudo nano /etc/rsyslog.d/49-haproxy.conf

```

*Nội dung file `/etc/rsyslog.d/49-haproxy.conf`:*

```
$ModLoad imudp
$UDPServerAddress 0.0.0.0
$UDPServerRun 514

local0.* -/var/log/haproxy.log
& stop

```

Khởi động lại dịch vụ:

```
sudo systemctl restart rsyslog

```

### 3. Cấu hình Filebeat đẩy log tới Kafka

```
sudo nano /etc/filebeat/filebeat.yml

```

*Nội dung file `/etc/filebeat/filebeat.yml`:*

```
filebeat.inputs:
- type: filestream
  id: haproxy-syslog
  enabled: true
  paths:
    - /var/log/haproxy.log
  fields:
    log_type: haproxy
  fields_under_root: true

output.kafka:
  hosts: ["192.168.153.120:9092"]
  topic: "haproxy-logs"
  partition.round_robin:
    reachable_only: false
  required_acks: 1
  compression: gzip
  max_message_bytes: 1000000

```

Khởi động lại Filebeat:

```
sudo systemctl restart filebeat

```

### 4. Tạo file cấu hình HAProxy

```
mkdir -p ~/elk-ingest/haproxy
nano ~/elk-ingest/haproxy/haproxy.cfg

```

*Nội dung file `~/elk-ingest/haproxy/haproxy.cfg`:*

```
global
    log 172.17.0.1:514 local0
    log 127.0.0.1:514 local0

defaults
    mode http
    timeout connect 10s
    timeout client  60s
    timeout server  60s

frontend kibana_frontend
    bind *:80
    default_backend kibana_backend

backend kibana_backend
    server kibana1 192.168.153.105:5601 check

```

### 5. Cấu hình Logstash Pipeline (`logstash.conf`)

```
nano ~/elk-ingest/logstash.conf

```

*Nội dung file `~/elk-ingest/logstash.conf`:*

```
input {
  kafka {
    bootstrap_servers => "kafka:9092"
    topics => ["haproxy-logs", "syslog"]
    codec => "json"
  }
}

filter {
  # Giải mã JSON từ Filebeat
  if [event][original] {
    json {
      source => "[event][original]"
    }
  }

  # Trích xuất real_message
  if [parsed_json][message] {
    mutate {
      add_field => { "real_message" => "%{[parsed_json][message]}" }
    }
  } else if [message] {
    mutate {
      add_field => { "real_message" => "%{message}" }
    }
  }

  # Bóc tách Grok log HAProxy
  if [real_message] =~ "haproxy" or [fields][log_type] == "haproxy" or [log][file][path] =~ "haproxy" {
    grok {
      match => { 
        "real_message" => "(?:%{SYSLOGTIMESTAMP}|%{TIMESTAMP_ISO8601}) %{HOSTNAME} haproxy\[%{INT}\]: %{IP:client_ip}:%{INT:client_port} \[%{NOTSPACE:accept_date}\] %{NOTSPACE:frontend_name} %{NOTSPACE:backend_name}/%{NOTSPACE:server_name} %{INT:time_request}/%{INT:time_queue}/%{INT:time_connect}/%{INT:time_response}/%{INT:time_total} %{INT:http_status} %{INT:bytes_read} - - ---- %{INT:actconn}/%{INT:feconn}/%{INT:beconn}/%{INT:srvconn}/%{INT:retries} %{INT:srv_queue}/%{INT:backend_queue} \"%{WORD:http_method} %{NOTSPACE:http_request} HTTP/%{NUMBER:http_version}\"" 
      }
      match => { 
        "real_message" => "%{IP:client_ip}:%{INT:client_port} \[%{NOTSPACE:accept_date}\] %{NOTSPACE:frontend_name} %{NOTSPACE:backend_name}/%{NOTSPACE:server_name} %{INT:time_request}/%{INT:time_queue}/%{INT:time_connect}/%{INT:time_response}/%{INT:time_total} %{INT:http_status} %{INT:bytes_read} - - ---- %{INT:actconn}/%{INT:feconn}/%{INT:beconn}/%{INT:srvconn}/%{INT:retries} %{INT:srv_queue}/%{INT:backend_queue} \"%{WORD:http_method} %{NOTSPACE:http_request} HTTP/%{NUMBER:http_version}\"" 
      }
      match => { "real_message" => "%{HAPROXYHTTP}" }
      tag_on_failure => ["_grokparsefailure_haproxy"]
    }

    mutate {
      convert => {
        "http_status" => "integer"
        "bytes_read" => "integer"
        "time_total" => "integer"
      }
    }
  }
}

output {
  elasticsearch {
    hosts => ["http://192.168.153.105:9200"]
    index => "logstash-%{+YYYY.MM.dd}"
  }
}

```

### 6. Khởi tạo `docker-compose.yml` trên VM 1

```
nano ~/elk-ingest/docker-compose.yml

```

*Nội dung file `~/elk-ingest/docker-compose.yml`:*

```
version: '3.8'

services:
  haproxy:
    image: haproxy:2.8
    container_name: haproxy
    ports:
      - "80:80"
    volumes:
      - ./haproxy/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro
    restart: always

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://192.168.153.120:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    depends_on:
      - zookeeper

  logstash:
    image: docker.elastic.co/logstash/logstash:8.11.0
    container_name: logstash
    volumes:
      - ./logstash.conf:/usr/share/logstash/pipeline/logstash.conf:ro
    depends_on:
      - kafka
    restart: always

```

Chạy dịch vụ:

```
cd ~/elk-ingest && docker compose up -d

```

## IV. BƯỚC 3: CÀI ĐẶT VÀ CẤU HÌNH TRÊN VM 2 (`dung-client` - 192.168.153.105)

### 1. Tối ưu Kernel cho Elasticsearch

```
sudo sysctl -w vm.max_map_count=262144
echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf

```

### 2. Khởi tạo `docker-compose.yml` trên VM 2

```
mkdir -p /home/ubuntu/elk-storage
nano /home/ubuntu/elk-storage/docker-compose.yml

```

*Nội dung file `/home/ubuntu/elk-storage/docker-compose.yml`:*

```
version: '3.8'

services:
  es01:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    container_name: es01
    environment:
      - node.name=es01
      - cluster.name=es-docker-cluster
      - discovery.type=single-node
      - bootstrap.memory_lock=true
      - "ES_JAVA_OPTS=-Xms512m -Xmx512m"
      - xpack.security.enabled=false
    ulimits:
      memlock:
        soft: -1
        hard: -1
    volumes:
      - esdata01:/usr/share/elasticsearch/data
    ports:
      - "9200:9200"
    networks:
      - elk-net
    restart: always

  kibana:
    image: docker.elastic.co/kibana/kibana:8.11.0
    container_name: kibana01
    ports:
      - "5601:5601"
    environment:
      ELASTICSEARCH_HOSTS: '["http://es01:9200"]'
    networks:
      - elk-net
    depends_on:
      - es01
    restart: always

volumes:
  esdata01:

networks:
  elk-net:
    driver: bridge

```

Chạy dịch vụ:

```
cd /home/ubuntu/elk-storage
docker compose down
docker compose up -d

```

## V. BƯỚC 4: KIBANA DATA VIEW & TRỰC QUAN HÓA

1. Truy cập Kibana qua đường dẫn: `http://192.168.153.120` (qua HAProxy) hoặc `http://192.168.153.105:5601`.

2. Mở menu **Menu ☰** $\rightarrow$ **Stack Management** $\rightarrow$ **Data Views** $\rightarrow$ Bấm **Create data view**.

3. Điền thông tin:

   * **Name**: `HAProxy Logs`

   * **Index pattern**: `logstash-*`

   * **Timestamp field**: `@timestamp`

   * **Custom filter**:

     ```
     fields.log_type : "haproxy" or parsed_json.fields.log_type : "haproxy" or real_message : *haproxy*
     
     ```

4. Lưu Data View và kiểm tra tại **Analytics** $\rightarrow$ **Discover**. các trường bóc tách chuẩn bao gồm: `client_ip`, `http_status`, `backend_name`, `http_method`, `http_request`.