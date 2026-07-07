# 학습 노트 (Knowledge)

> **이 폴더의 역할**: 이 프로젝트를 하며 **배운 개념·삽질 회고**를 정리한다. 프로젝트의
> 설계 결정(무엇을 채택했나)은 여기가 아니라 [`../DESIGN.md`](../DESIGN.md)로 간다. 여기는
> "왜 이 개념이 이렇게 동작하는가"처럼 재사용 가능한 지식이 사는 곳.

## 파일 규칙
- 파일명: `{주제-슬러그}.md` (예: `innodb-locking.md`, `outbox-pattern.md`)
- 개념 → 왜 그런가 → 이 프로젝트에서 어떻게 썼나(링크) 순으로.

## 후보 주제 (예시)
- InnoDB row lock 과 REPEATABLE READ 스냅샷 vs 잠금 읽기
- outbox 패턴과 at-least-once, 컨슈머 멱등성
- Kafka DLT / 에러 핸들링
