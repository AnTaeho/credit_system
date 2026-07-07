# 문서 지도 (Documentation Map)

이 프로젝트의 모든 문서와 그 **역할 경계**를 정의하는 인덱스다. 새 내용을 어디에 쓸지
헷갈리면 여기서 목적지를 찾는다. **한 사실은 한 문서에만** 산다(중복 금지).

## 지도

| 문서 | 담는 것 (한 줄) | 담지 않는 것 → 이동처 | 독자 |
|---|---|---|---|
| `README.md` (루트) | 프로젝트가 무엇이고 **어떻게 실행**하는가 + 문서 링크 | 설계 이유 → `docs/DESIGN.md` | 처음 온 사람 |
| `docs/DESIGN.md` | 아키텍처·도메인 모델·상태머신·**정합성 불변식**·설계 결정(**why**) | 무엇이 문제였나(상태) → `PRODUCTION_READINESS` | 구현·리뷰어 |
| `docs/PRODUCTION_READINESS.md` | 검토 결과의 **상태**: 심각도 태깅 + 해결/보류 | 개별 이슈의 심층 분석 → `docs/analysis/` | 리뷰어·의사결정 |
| `docs/analysis/*.md` | 이슈 **한 건의 PAR 심층**(Problem→Analysis→Action→Result) | 상태 요약 → `PRODUCTION_READINESS` | 깊게 파는 사람 |
| `docs/knowledge/*.md` | **배운 것**·회고·개념 정리 | 프로젝트 결정 사항 → `DESIGN` | 학습·미래의 나 |
| `PROJECT_STRUCTURE_GUIDE.md` (루트) | **일하는 방식**(협업·구조·품질) — 스택 초월 메타 지침 | 이 프로젝트 고유 내용 → 위 문서들 | 모든 프로젝트 |
| `.claude/CLAUDE.md` | Claude 협업 지침 + 프로젝트 지침 (**설정**, 자동 로드) | 사람이 읽는 설명 → 위 문서들 | Claude(Advisor) |
| `.claude/agents/worker.md` | Worker 서브에이전트 정의 (**설정**) | — | Claude(Worker) |

## "어디에 쓸까" 결정 규칙

- 실행 방법·빠른 시작 → **README**
- "왜 이렇게 설계했나" → **DESIGN**
- "무엇이 문제고 해결됐나/보류인가"(상태) → **PRODUCTION_READINESS**
- 특정 이슈를 P·A·R로 깊게 → **docs/analysis/{이슈}.md**
- 오늘 배운 개념/삽질 회고 → **docs/knowledge/{주제}.md**
- 일반적 작업 방식·컨벤션 → **PROJECT_STRUCTURE_GUIDE** (프로젝트 고유 아님)
- Claude가 자동으로 따를 규칙 → **.claude/CLAUDE.md**

## 경계 규칙

1. **한 사실은 한 문서에만.** 다른 문서는 링크로 참조한다(복붙 금지).
2. **상태 ↔ 분석 분리.** `PRODUCTION_READINESS`는 "무엇이 어떤 상태인가"(짧게), 깊은 근거는
   `docs/analysis/`로 링크. 요약본과 심층본을 한 문서에 섞지 않는다.
3. **설계(결정) ↔ 리뷰(상태) 분리.** 왜 그렇게 만들었나는 DESIGN, 그게 프로덕션 기준에
   부합하나는 PRODUCTION_READINESS.
4. **메타 ↔ 고유 분리.** 일하는 방식은 PROJECT_STRUCTURE_GUIDE(모든 프로젝트 공통),
   이 프로젝트 내용은 docs/ 하위.

## 현황 (skeleton)

이 시스템은 새로 구축됐다. 각 문서는 역할 헤더와 아웃라인을 갖춘 골격 상태이며, 내용은
작업하며 채운다. 채움 우선순위: `README` → `DESIGN` → `PRODUCTION_READINESS`.
