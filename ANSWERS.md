# Day 28 Track 2 — cá nhân

## Phạm vi và ownership

Tôi thực hiện cá nhân toàn bộ các vai trò trong integration matrix: ingestion/
orchestration (IP01–IP02), data & ML (IP03, IP04, IP06), serving/retrieval
(IP05, IP07) và platform/observability (IP08–IP10). Sơ đồ kiến trúc dùng khi
demo là [`docs/images/lab28-architecture-overview.svg`](docs/images/lab28-architecture-overview.svg):
gateway là entry point; API ghi event vào Kafka; Airflow điều phối Spark/Delta,
Feast và Qdrant; MLflow quản lý release; OTEL/Prometheus/Grafana quan sát hệ
thống.

## Quyết định kỹ thuật và trade-off

- Kafka mang `traceparent` và idempotency key; consumer chỉ commit offset sau
  khi xử lý durable. Cách này là at-least-once, ưu tiên không mất dữ liệu hơn
  là exactly-once ở broker.
- Delta deduplicate theo `idempotency_key`, chọn bản ghi mới nhất bằng
  `(occurred_at, event_id)`. Vì vậy replay Kafka có thể tạo một Delta MERGE mới
  nhưng không làm tăng số row logic.
- Airflow dùng một consumer group và `max_active_runs=1`, để một batch có quan
  hệ rõ ràng với một Delta version/asset event. Trade-off là throughput thấp
  hơn khi backlog lớn.
- Feature export pin Delta version trước khi Feast materialize; Qdrant dùng
  deterministic point ID từ `doc_id`. Cả hai lựa chọn làm evidence và replay
  có thể đối chiếu, thay vì phụ thuộc thời điểm đọc.
- Gateway giới hạn 10 RPS. Đây là guardrail để bảo vệ downstream; client cần
  retry/backoff thay vì coi 429 là lỗi hạ tầng.
- Readiness phân biệt `ready`, `degraded` và `not_ready`. vLLM là dependency
  optional với local core path, nhưng là gate bắt buộc khi khẳng định IP07.

## Happy path đã xác minh

J1 chạy thành công với trace ID `39abe2a5d0fa440785f6b07fe45a8dce`, Airflow run
`it-6cb12413`, và tất cả bốn task đều `success`. Cùng trace xuất hiện ở Kafka,
Airflow và Spark; Feast trả entity `it-j1-b6b1e42a` với `feedback_count=1` và
`delta_version=4`. Champion MLflow tại thời điểm thu evidence là
`lab28-rag-release` version `1`, run
`df84382730df45f8a10d37bf36197f8f`.

Evidence runtime nằm trong `evidence/` (thư mục bị Git ignore để không vô tình
nộp dữ liệu/cached artefacts):

- `ip01-kafka-consume.json`, `ip02-airflow-run.json`, `ip04-feast-online.json`
  chứng minh Kafka, DAG/asset và Feast online.
- `ip03-delta-history.json`, `ip05-qdrant-search.json`,
  `ip06-mlflow-release.json` chứng minh lakehouse, retrieval và release.
- `ip08-gateway.json`, `ip09-prometheus-targets.json`,
  `ip09-grafana-dashboards.json`, `ip10-trace.json` là evidence do integration
  tests truy vấn từ các control plane thật.
- `ip07-vllm-identity.json` ghi trung thực trạng thái `reachable: false` và
  `is_real_vllm: false`; không có mock thay thế kết quả này.

## Correctness, recovery và observability

Các lệnh đã chạy trên stack Docker full:

```text
uv run pytest starter-tests -q                         # 4 passed
uv run pytest tests -q                                 # 83 passed
uv run pytest integration-tests/test_j1_golden_path.py -q  # 12 passed, 3 GPU skipped
uv run pytest integration-tests/test_j2_idempotent_replay.py -q  # 9 passed
uv run pytest integration-tests -m "not gpu and not langsmith" -q  # 56 passed, 16 deselected
```

J2 là bằng chứng replay/idempotency. J4 trong suite full kiểm tra dependency
failure, DLQ/replay và recovery không mất dữ liệu. J5 sinh evidence gateway,
Prometheus/Grafana và local trace. Alert rules đang có `Lab28ApiUnavailable`
(`critical`) và `Lab28HighErrorRatio` (`warning`); targets cần thiết đều `up`,
trừ target vLLM optional do chưa có endpoint GPU.

## Profile tải và phân tích bottleneck

Môi trường Docker có giới hạn bộ nhớ khả dụng 15.62 GiB. Các số dưới đây là
baseline `/ready`, không phải capacity claim cho production và không đo
`/api/v1/ask` vì chưa có vLLM thật.

| Route / concurrency | Kết quả | P50 | P95 | P99 |
|---|---:|---:|---:|---:|
| Gateway `:8080`, 8 workers | 12/200 HTTP 200 | 0.83 ms | 392.24 ms | 573.40 ms |
| Gateway `:8080`, 16 workers | 4/200 HTTP 200 | 6.38 ms | 101.53 ms | 625.40 ms |
| API `:8000`, 8 workers | 200/200 HTTP 200 | 406.06 ms | 511.33 ms | 548.61 ms |
| API `:8000`, 16 workers | 200/200 HTTP 200 | 731.12 ms | 980.88 ms | 1060.42 ms |

Script profile hiện quy HTTP error (như gateway 429) về status `0`; vì vậy hai
profile gateway thể hiện rate-limit saturation, không phải network disconnect.
Evidence IP08 xác nhận policy 10 RPS với một mẫu 30 request có 10 accepted và
20 rejected. Sau profile, snapshot cho thấy API khoảng 239 MiB, Spark Connect
khoảng 1.96 GiB, Airflow khoảng 1.51 GiB và MLflow khoảng 2.09 GiB; CPU của API
và MLflow còn cao ngay sau thử tải. Bottleneck ưu tiên xử lý là quota/rate limit
ở edge, sau đó là latency tăng theo concurrency tại API/MLflow. Bước tiếp theo
trong production là load test một corpus `/api/v1/ask` với vLLM thật, đo queue,
token throughput, Kafka lag, error rate và CPU/RAM theo chuỗi thời gian.

## Kubernetes, GitOps và rollback

`uv run python scripts/validate_manifests.py` đã pass, còn
`scripts/check_portability.py` pass cho workflow không phụ thuộc host path/shell.
Model promotion/rollback đã được kiểm tra qua J3 trong full non-GPU suite: alias
`champion` chuyển release và quay về phiên bản trước mà không sửa application
code. Quy trình desired-state rollback được ghi ở
[`runbooks/gitops-rollback.md`](runbooks/gitops-rollback.md).

**UNVERIFIED:** môi trường hiện không có Kubernetes cluster hoặc Argo CD được
kết nối, vì vậy chưa thể trung thực tạo drift rồi quan sát self-heal/sync thực.
Chỉ manifest contract validation và model-registry rollback được khẳng định.
Khi có cluster, cần build image immutable, đổi tag qua Git, Argo sync, tạo drift
được ghi nhận, rồi revert Git revision và thu health/gateway/trace evidence.

Lưu ý khi trình bày local core: API trong Compose đặt
`LAB28_VLLM_REQUIRE_REAL=false`, nên `/ready` trả `degraded` nếu chỉ vLLM thiếu.
CLI chạy trên host mặc định gate này là `true`, nên cần chạy
`LAB28_VLLM_REQUIRE_REAL=false uv run lab28 ready` để kiểm tra cùng local-core
policy. Đây không xác minh IP07; GPU compose và Kubernetes vẫn ép giá trị `true`.

## Production gaps và bước tiếp theo

1. Cần endpoint vLLM GPU thật, với `/version`, `/v1/models` và metric `vllm:`;
   sau đó chạy lại GPU-gated J1/J3/J4. IP07 hiện là **UNVERIFIED**, không được
   trình bày như đã pass.
2. Cần `LANGSMITH_API_KEY` nếu muốn xác minh leg LangSmith; local OTLP/Jaeger
   hiện là bằng chứng trace deterministic của lab.
3. Tách gateway load profile thành các mức dưới/vượt quota và implement retry
   policy ở client. Không suy diễn capacity production từ laptop này.
4. Chạy GitOps drift/self-heal trên cluster thật và lưu revision/image digest
   cùng evidence rollback.
5. Không commit `.env`, token, database/cache, model weights hoặc `.lab28/`.
   Evidence runtime được kiểm tra trước khi đóng gói nộp.
