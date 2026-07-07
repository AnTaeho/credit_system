# 심층 분석 (Analysis) — PAR

**공통 원칙**: check-then-act를 배제하고, 조건부 UPDATE 한 문장으로 확인+실행을 원자화한다.

> **이 폴더의 역할**: 이슈 **한 건**을 `Problem → Analysis → Action → Result` 구조로 깊게
> 분석한 문서를 모은다. 파일 하나 = 이슈 하나. 상태 요약은 여기 두지 않고
> [`../PRODUCTION_READINESS.md`](../PRODUCTION_READINESS.md)에서 이 문서로 링크한다.

## 파일 규칙
- 파일명: `{번호}-{짧은-슬러그}.md` (예: `01-optimistic-lock-retry.md`)
- 근거 커밋 해시를 반드시 포함한다.

## PAR 템플릿

```markdown
# {제목} ({심각도}) — `{commit}`

## P — Problem
무엇이 문제였나. 재현 조건/코드 인용.

## A — Analysis
왜 그랬나. 근본 원인(메커니즘 수준).

## Action
무엇으로 고쳤나. 핵심 diff/스니펫.

## R — Result
결과·검증(테스트). 남은 트레이드오프.
```
