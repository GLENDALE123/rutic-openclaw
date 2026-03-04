# E2E 검증: Oracle 게이트웨이 ↔ home1 에이전트 연동

> **목표**: home1에서 실행한 에이전트가 Oracle NATS를 통해 OpenClaw 게이트웨이에 연결되고,
> config 수신 → task 처리 → reply 반환까지 전체 플로우가 동작하는지 확인.

---

## 아키텍처

```
home1 (에이전트)
  └─ agent-entry.ts
       ├─ _rpc.config.request → Oracle:4222 → OpenClaw config server → config 수신
       ├─ _rpc.agent.register → 에이전트 등록
       ├─ task.{agentId} 구독 → 태스크 수신
       └─ reply.send 발행 → 응답 전달
              ↕ NATS (nats://100.75.107.81:4222)
Oracle (100.75.107.81)
  ├─ NATS 서버 :4222
  └─ OpenClaw 게이트웨이 (Docker)
       ├─ gateway-config-server: _rpc.config.request 응답
       └─ NATS transport: task.{gateway-agentId} 구독
```

---

## 1단계: Oracle — OpenClaw 게이트웨이 시작

### `.env` 설정 (Oracle)

```bash
# Oracle에서 실행할 .env 파일
OPENCLAW_GATEWAY_TOKEN=<토큰>
OPENCLAW_CONFIG_DIR=/home/ubuntu/.openclaw   # config 파일 위치

# RUTIC NATS 트랜스포트 활성화 (이게 없으면 config server 미시작)
RUTIC_AGENT_ID=gateway                        # 게이트웨이 자신의 agentId
RUTIC_NATS_URL=nats://localhost:4222          # Oracle 로컬 NATS
```

### 실행

```bash
# Oracle에서
cd /path/to/rutic-openclaw
docker compose up -d openclaw-gateway

# 로그 확인 — "nats-config-server: started" 메시지 확인
docker compose logs -f openclaw-gateway | grep -E "nats|rutic"
```

**정상 로그 예시**:

```
nats: gateway → nats://localhost:4222
nats-transport: agent=gateway ready
nats-config-server: started (_rpc.config.request 대기 중)
```

---

## 2단계: home1 — 에이전트 실행

### 방법 A: Docker (권장)

```bash
# home1에서 — rutic-openclaw 레포 기준
docker build -f Dockerfile.agent -t rutic-agent .

# Tailscale MagicDNS 사용 (Oracle 머신명만 입력)
docker run --rm \
  -e RUTIC_AGENT_ID=test-agent \
  -e RUTIC_NATS_TAILSCALE_HOST=oracle \
  rutic-agent

# 또는 IP 직접 지정
docker run --rm \
  -e RUTIC_AGENT_ID=test-agent \
  -e RUTIC_NATS_URL=nats://100.75.107.81:4222 \
  rutic-agent
```

> **참고**: 빌드 출력 경로는 `dist-agent/src/nats/agent-entry.js` (tsconfig rootDir 미설정으로 src/ 포함)

### 방법 B: 직접 실행 (개발 중)

```bash
# home1에서 (pnpm 설치된 경우)
RUTIC_AGENT_ID=test-agent \
RUTIC_NATS_TAILSCALE_HOST=oracle \
pnpm agent
```

**정상 로그 예시**:

```
nats: test-agent → nats://100.75.107.81:4222
config-sync [test-agent]: requesting config from gateway...
config-sync [test-agent]: config received (XXXX bytes)
agent-entry [test-agent]: config 적용 완료
agent-entry [test-agent]: 준비 완료 — task.test-agent 대기 중
```

---

## 3단계: 연동 검증

### 3-1. config 수신 확인

에이전트 로그에서 "config received" 메시지 확인 → config server 정상 동작.

### 3-2. task → reply 플로우 확인

Oracle 또는 WSL에서 NATS CLI로 테스트:

```bash
# NATS CLI 설치: https://github.com/nats-io/natscli
# reply.send 구독 먼저 열기 (터미널 1)
nats sub reply.send --server nats://100.75.107.81:4222

# task 발행 (터미널 2)
nats pub task.test-agent \
  '{"code":"EVT_TASK_CREATED","taskId":"t-001","agentId":"test-agent","body":"ping","timestamp":0}' \
  --server nats://100.75.107.81:4222
```

**정상 응답 예시** (터미널 1):

```json
{
  "code": "EVT_REPLY_SEND",
  "taskId": "t-001",
  "agentId": "test-agent",
  "text": "...",
  "isFinal": true,
  "timestamp": ...
}
```

### 3-3. 에이전트 등록 확인

```bash
# _rpc.agent.register 구독 (등록 이벤트 확인)
nats sub "_rpc.agent.register" --server nats://100.75.107.81:4222
```

---

## 체크리스트

- [ ] Oracle: `docker compose logs` 에서 `nats-config-server: started` 확인
- [ ] home1: 에이전트 로그에서 `config received` 확인
- [ ] home1: 에이전트 로그에서 `task.test-agent 대기 중` 확인
- [ ] `nats pub task.test-agent` → `reply.send` 응답 수신 확인
- [ ] 에이전트 재시작 후 `config.updated` 핫리로드 동작 확인 (선택)

---

## 트러블슈팅

| 증상                            | 원인                                                | 해결                                                 |
| ------------------------------- | --------------------------------------------------- | ---------------------------------------------------- |
| `config received` 없이 타임아웃 | Oracle RUTIC_AGENT_ID 미설정 → config server 미시작 | Oracle .env에 `RUTIC_AGENT_ID` 추가 후 재시작        |
| NATS 연결 실패                  | 방화벽 / 포트 차단                                  | Oracle에서 4222 포트 오픈 확인                       |
| config 파싱 실패                | OpenClaw config 파일 없음                           | `OPENCLAW_CONFIG_DIR` 경로에 config 파일 있는지 확인 |
| task 발행 후 reply 없음         | 에이전트 config 로드 실패로 런타임 미초기화         | 에이전트 로그에서 "config 적용 완료" 확인            |
