# TÀI LIỆU HƯỚNG DẪN DỰNG HỆ THỐNG GIÁM SÁT KẾT HỢP LOG HAPROXY VỚI MÔ HÌNH ELK STACK & KAFKA

Tài liệu tổng hợp quy trình từng bước triển khai hệ thống thu thập, phân luồng và phân tích log HAProxy tập trung từ đầu đến hoàn thiện.

---

## I. MÔ HÌNH TỔNG QUAN VÀ THÔNG TIN MÔI TRƯỜNG

### 1. Kiến trúc luồng dữ liệu (Log Pipeline)
$$\text{HAProxy Logs} \xrightarrow{\text{UDP 514}} \text{rsyslog (/var/log/haproxy.log)} \xrightarrow{\text{Filebeat}} \text{Kafka (Topic: haproxy-logs)} \xrightarrow{\text{Logstash (Grok)}} \text{Elasticsearch} \xrightarrow{\text{Kibana}}$$

### 2. Phân bổ hạ tầng
* **VM 1 (`student08-server` - IP: `192.168.153.120`)**:
  * Dịch vụ OS: `rsyslog`, `Filebeat`.
  * Container Docker: `HAProxy`, `Kafka`, `Logstash`.
* **VM 2 (`dung-client` - IP: `192.168.153.105`)**:
  * Container Docker: `Elasticsearch` (1 Node - `es01`), `Kibana` (`kibana01`).

---

## II. TRIỂN KHAI VÀ CẤU HÌNH TRÊN VM 1 (`student08-server` - 192.168.153.120)

### Bước 1: Khởi tạo file log và Cấu hình `rsyslog` lắng nghe UDP 514
1. **Tạo file log và phân quyền:**
   ```bash
   sudo touch /var/log/haproxy.log
   sudo chown syslog:adm /var/log/haproxy.log
   sudo chmod 644 /var/log/haproxy.log
   ```

2. **Cấu hình `rsyslog` nhận log từ UDP:**
   Mở file `/etc/rsyslog.d/49-haproxy.conf`:
   ```bash
   sudo nano /etc/rsyslog.d/49-haproxy.conf
   ```
   Nội dung file:
   ```rsyslog
   $ModLoad imudp
   $UDPServerAddress 0.0.0.0
   $UDPServerRun 514

   local0.* -/var/log/haproxy.log
   & stop
   ```

3. **Restart dịch vụ `rsyslog`:**
   ```bash
   sudo systemctl restart rsyslog
   ```

---

### Bước 2: Cấu hình HAProxy
Mở file `~/elk-ingest/haproxy/haproxy.cfg`:
```bash
nano ~/elk-ingest/haproxy/haproxy.cfg
```
Nội dung file:
```haproxy
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

---

### Bước 3: Cấu hình Filebeat
Mở file `/etc/filebeat/filebeat.yml`:
```bash
sudo nano /etc/filebeat/filebeat.yml
```
Nội dung file:
```yaml
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
```bash
sudo systemctl restart filebeat
```

---

### Bước 4: Cấu hình Logstash Pipeline (`logstash.conf`)
Mở file `~/elk-ingest/logstash.conf`:
```bash
nano ~/elk-ingest/logstash.conf
```
Nội dung file:
```ruby
input {
  kafka {
    bootstrap_servers => "kafka:9092"
    topics => ["haproxy-logs", "syslog"]
    codec => "json"
  }
}

filter {
  # 1. Giải mã JSON từ Filebeat (nếu bị bọc trong event.original)
  if [event][original] {
    json {
      source => "[event][original]"
    }
  }

  # 2. Chuẩn hóa chuỗi message
  if [parsed_json][message] {
    mutate {
      add_field => { "real_message" => "%{[parsed_json][message]}" }
    }
  } else if [message] {
    mutate {
      add_field => { "real_message" => "%{message}" }
    }
  }

  # 3. Bóc tách thông tin log HAProxy bằng Grok
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

Khởi động lại Logstash & HAProxy:
```bash
cd ~/elk-ingest
docker compose restart haproxy logstash
```

---

## III. TRIỂN KHAI VÀ CẤU HÌNH TRÊN VM 2 (`dung-client` - 192.168.153.105)

### Cấu hình Docker Compose (`docker-compose.yml`)
Mở file `/home/ubuntu/elk-storage/docker-compose.yml`:
```bash
nano /home/ubuntu/elk-storage/docker-compose.yml
```
Nội dung file (Đã tối ưu 1 node Elasticsearch và gán Docker Network `elk-net`):

```yaml
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

volumes:
  esdata01:

networks:
  elk-net:
    driver: bridge
```

Khởi chạy lại cụm container trên VM 2:
```bash
cd /home/ubuntu/elk-storage
docker compose down
docker compose up -d
```

---

## IV. CẤU HÌNH KIBANA DATA VIEW & HIỂN THỊ LOG

### Tạo Data View trên Kibana UI
1. Truy cập Kibana qua địa chỉ: `http://kibana.lab.local` (hoặc `http://192.168.153.105:5601`).
2. Vào **Menu ☰** $\rightarrow$ **Stack Management** $\rightarrow$ **Data Views** $\rightarrow$ bấm **Create data view**.
3. Điền thông tin cấu hình:
   * **Name**: `HAProxy Logs`
   * **Index pattern**: `logstash-*`
   * **Timestamp field**: `@timestamp`
   * **Advanced options / Custom filter**:
     ```text
     fields.log_type : "haproxy" or parsed_json.fields.log_type : "haproxy" or real_message : *haproxy*
     ```
4. Bấm **Save data view to Kibana**.

### Các trường dữ liệu đã được bóc tách (Parsed Fields)
Vào menu **Analytics** $\rightarrow$ **Discover**, chọn Data View **`HAProxy Logs`** để truy vấn các field chuẩn:
* `client_ip`: IP người dùng gửi request.
* `client_port`: Cổng gửi của client.
* `http_method`: Phương thức HTTP (`GET`, `POST`,...).
* `http_request`: Đường dẫn URL tài nguyên.
* `http_status`: Mã phản hồi HTTP (`200`, `404`, `503`,...).
* `backend_name`: Tên backend xử lý trên HAProxy.
* `frontend_name`: Tên frontend nhận request trên HAProxy.