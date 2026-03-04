# delegate-task

다른 RUTIC 에이전트에게 태스크를 위임하고 결과를 받아온다.

## 언제 사용

- 다른 에이전트의 전문성이 필요할 때
  - 재무 분석이 필요하면 → `cfo`
  - 기술 검토가 필요하면 → `cto`
  - 시장조사가 필요하면 → `researcher`
  - 운영 계획이 필요하면 → `coo`
- 여러 에이전트에게 동시에 의견을 구할 때 (병렬 bash 호출)
- 내 역할 범위 밖의 판단이 필요할 때

## 사용법

```bash
node --import tsx ~/rutic-openclaw/src/nats/delegate.ts <agentId> "<message>"
```

결과는 stdout, 디버그 로그는 stderr로 출력된다.

## 에이전트 목록

| agentId    | 역할                                   |
| ---------- | -------------------------------------- |
| researcher | 리서처 — 시장조사, 데이터 분석, 트렌드 |
| ceo        | CEO — 전략 결정, 최종 판단             |
| cfo        | CFO — 재무 분석, 예산, ROI             |
| cto        | CTO — 기술 검토, 아키텍처              |
| coo        | COO — 운영 계획, 프로세스              |
| cmo        | CMO — 마케팅 전략, 브랜딩              |
| chro       | CHRO — 인사, 채용, 조직                |
| pm         | PM — 프로젝트 관리, 일정               |
| risk       | Risk Manager — 리스크 분석             |

## 예시

```bash
# CFO에게 재무 분석 요청
result=$(node --import tsx ~/rutic-openclaw/src/nats/delegate.ts cfo \
  "이 프로젝트 초기 투자 $50k, 월 예상 수익 $20k 기준으로 3개월 수익성 분석해줘")
echo "$result"

# 여러 에이전트에게 병렬로 의견 수집
cfo_result=$(node --import tsx ~/rutic-openclaw/src/nats/delegate.ts cfo "재무 타당성 검토" &)
cto_result=$(node --import tsx ~/rutic-openclaw/src/nats/delegate.ts cto "기술 실현 가능성 검토" &)
wait

# 타임아웃 조정 (기본 120초)
node --import tsx ~/rutic-openclaw/src/nats/delegate.ts cto "복잡한 아키텍처 검토" --timeout 180000
```

## 주의사항

- 위임받는 에이전트가 현재 실행 중이어야 응답 가능
- 각 위임 호출은 별도 세션으로 처리됨 (히스토리 공유 없음)
- `--session <key>` 옵션으로 세션 키를 직접 지정하면 히스토리 공유 가능
- 기본 타임아웃: 120초
