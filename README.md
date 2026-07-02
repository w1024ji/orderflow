# OrderFlow 
— Real-time Crypto Orderbook Imbalance Pipeline

> 실시간 암호화폐 호가(orderbook) 스트림에서 **가중 호가 불균형(weighted orderbook imbalance)** 지표를
1초 단위로 계산하고, 핫 경로(실시간 서빙)와 콜드 경로(분석 적재)로 분리해 제공하는
스트리밍 데이터 플랫폼. GitOps로 배포하고, 전 구간을 관측 가능하게 구성한다.

## 1. 아키텍처

```
=============================================================================
 Flow 1: GitOps CI/CD
=============================================================================
[로컬 PC] ──git push──► [GitHub Actions] ──image push──► [Container Registry]
                         (lint·test·build)                      │
                                                                ▼ (자동 감지 & sync)
                                                        [ ArgoCD ] @ argocd ns
                                                                │  (K8s 상태 동기화)
                                                                ▼
=============================================================================
 Flow 2: Real-time Data Pipeline  @ data-pipeline ns
=============================================================================
[Binance WebSocket API]  (depth snapshot + diff 스트림)
        │
        ▼
[Python Producer (Pod)]
   - 오더북 재구성(snapshot+diff, 시퀀스 갭 감지, 자동 재연결)
   - Avro 직렬화 → Schema Registry 등록
   - symbol을 key로 파티셔닝
        │
        ├──────────────► [Kafka topic: orderbook.raw]   (원본 보존 = replay 소스)
        │
        ▼
[Apache Kafka (Pod x3, KRaft)]  (버퍼링 · 파티션 단위 순서 보장 · 백프레셔 흡수)
        │
        ▼
[Apache Flink (Pod)]
   - event-time 1초 Tumbling Window
   - 가중 호가 불균형 연산
   - RocksDB state backend + S3 체크포인트
   - 깨진 메시지 → DLQ
        │
        ├─► [Sink 1: Redis] ───────► [Serving API] ──► [Frontend Dashboard]
        │     (symbol별 최신 지표        (Redis 읽기/          (실시간 지표)
        │      O(1) upsert, TTL)         pub-sub push)
        │
        └─► [Sink 2: Amazon S3] ────► [Amazon Athena / BI]
              (Parquet, 시간/심볼          (과거 데이터 통계·패턴 분석,
               파티셔닝, 파일 롤링)          partition projection)
=============================================================================
 Flow 3: Observability  @ monitoring ns
=============================================================================
[Producer / Kafka / Flink 파드] ──metrics──► [Prometheus (Pod)]
   (CPU·메모리·네트워크,                          (시계열 저장)
    consumer lag, 체크포인트 지표)                    │
                                                     ▼ query
                                            [Grafana (Pod)] ──► [Infra Dashboard]
                                            (대시보드 + 장애 알림)
=============================================================================
