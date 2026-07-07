# 프로덕션 준비도 검토 (Production Readiness)

> **이 문서의 역할**: 검토 결과의 **상태**를 심각도 태깅과 해결/보류로 관리한다. 짧게
> 유지하고, 개별 이슈의 깊은 근거(Problem→Analysis→Action→Result)는 `analysis/{이슈}.md`로
> 링크한다. → 경계는 [`docs/README.md`](./README.md) 참고. 형식 표준은
> [`PROJECT_STRUCTURE_GUIDE.md`](../PROJECT_STRUCTURE_GUIDE.md) 3장.

## 총평
- 핵심 불변식이 지켜지는가 (한 문단)
- 검토일 / 갱신 이력

## ✅ 해결됨
- `#N. 제목` (심각도) — 근거 커밋 `hash`. 한 줄 요약 + 심층 링크 → `analysis/{이슈}.md`

## 🟠 보류 (의도적 미수정)
- `항목` — **보류 이유와 조건** 명시 (예: "로컬 데모 전용, 실배포 시 필수")

## 🟡 Minor / 개선
- `항목` — 해결 or 개선 방향

## 심각도 기준
- **Blocker**: 핵심 불변식 파괴 · **Major**: 신뢰성/보안 · **Minor**: 개선
