"""
srlDB_with_senseid.sqlite3 에서
- sentences: sentence_form 이 같은 행
- predicates: sentence_id, predicate_form, predicate_begin, predicate_end 가 같은 행
을 찾아 출력합니다.
"""
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "srlDB_with_senseid.sqlite3"


def find_duplicate_predicates(
    db_path: str | Path, limit: int | None = 50
) -> list[tuple]:
    """
    predicates 테이블에서 sentence_id, predicate_form, predicate_begin, predicate_end
    가 모두 같은 행(중복)을 찾습니다.
    반환: [(sentence_id, predicate_form, predicate_begin, predicate_end, 개수, predicate_id들), ...]
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DB 파일 없음: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        q = """
        SELECT sentence_id, predicate_form, predicate_begin, predicate_end,
               COUNT(*) AS cnt, GROUP_CONCAT(predicate_id) AS ids
        FROM predicates
        GROUP BY sentence_id, predicate_form, predicate_begin, predicate_end
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        """
        if limit is not None:
            q += f" LIMIT {int(limit)}"
        cur = conn.execute(q)
        return [tuple(row) for row in cur.fetchall()]
    finally:
        conn.close()


def delete_duplicate_predicates(db_path: str | Path) -> int:
    """
    predicates 테이블에서 sentence_id, predicate_form, predicate_begin, predicate_end 가
    같은 중복 행 중 predicate_id 가 큰 행만 삭제합니다. (가장 작은 predicate_id 는 유지)
    arguments 테이블은 FK CASCADE 로 함께 삭제됩니다.
    반환: 삭제된 행 수.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DB 파일 없음: {db_path}")
    conn = sqlite3.connect(db_path)
    try:
        # 중복 그룹별로 predicate_id 목록 조회 (정렬해서 최소만 남기고 나머지 수집)
        cur = conn.execute("""
            SELECT sentence_id, predicate_form, predicate_begin, predicate_end,
                   GROUP_CONCAT(predicate_id) AS ids
            FROM predicates
            GROUP BY sentence_id, predicate_form, predicate_begin, predicate_end
            HAVING COUNT(*) > 1
        """)
        ids_to_delete: list[int] = []
        for row in cur.fetchall():
            ids = [int(x) for x in row[4].split(",")]
            keep = min(ids)
            ids_to_delete.extend(i for i in ids if i != keep)
        if not ids_to_delete:
            return 0
        conn.execute("PRAGMA foreign_keys = ON")
        # SQLite 변수 개수 제한(999 등)을 피하기 위해 배치로 삭제
        batch_size = 500
        for i in range(0, len(ids_to_delete), batch_size):
            batch = ids_to_delete[i : i + batch_size]
            conn.execute(
                "DELETE FROM predicates WHERE predicate_id IN ({})".format(
                    ",".join("?" * len(batch))
                ),
                batch,
            )
        conn.commit()
        return len(ids_to_delete)
    finally:
        conn.close()


def find_duplicate_sentences(db_path: str | Path, limit: int | None = 50) -> list[tuple]:
    """
    sentence_form 이 동일한 문장(중복)을 찾습니다.
    반환: [(sentence_form, 개수, sentence_id들), ...]
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DB 파일 없음: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        q = """
        SELECT sentence_form, COUNT(*) AS cnt, GROUP_CONCAT(sentence_id) AS ids
        FROM sentences
        GROUP BY sentence_form
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        """
        if limit is not None:
            q += f" LIMIT {int(limit)}"
        cur = conn.execute(q)
        return [tuple(row) for row in cur.fetchall()]
    finally:
        conn.close()


def main() -> None:
    args = sys.argv[1:]
    mode = "sentences"
    limit = 50
    do_delete = False
    if args and args[0].lower() in ("predicates", "p", "predicate"):
        mode = "predicates"
        args = args[1:]
    if "--delete" in args:
        do_delete = True
        args = [a for a in args if a != "--delete"]
    if args:
        try:
            limit = int(args[0])
        except ValueError:
            limit = None

    try:
        if mode == "predicates" and do_delete:
            n = delete_duplicate_predicates(DB_PATH)
            print(f"중복 predicate 삭제 완료: {n}개 행 삭제됨.")
            return
        if mode == "predicates":
            rows = find_duplicate_predicates(DB_PATH, limit=limit)
            if not rows:
                print("sentence_id, predicate_form, predicate_begin, predicate_end 가 같은 행이 없습니다. (중복 없음)")
                return
            print(f"predicates 동일 그룹 (총 {len(rows)}개, 상위 제한: {limit or '없음'})\n")
            print("-" * 70)
            for sentence_id, pred_form, pred_begin, pred_end, cnt, ids in rows:
                print(f"sentence_id: {sentence_id}")
                print(f"  predicate_form: {pred_form!r}  |  begin: {pred_begin}, end: {pred_end}")
                print(f"  동일 개수: {cnt}  |  predicate_id: {ids}")
                print("-" * 70)
        else:
            rows = find_duplicate_sentences(DB_PATH, limit=limit)
            if not rows:
                print("sentence_form 이 같은 행이 없습니다. (중복 없음)")
                return
            print(f"sentence_form 이 같은 문장 (총 {len(rows)}개 그룹, 상위 제한: {limit or '없음'})\n")
            print("-" * 60)
            for sentence_form, cnt, ids in rows:
                print(f"[문장] {sentence_form[:80]}{'...' if len(sentence_form) > 80 else ''}")
                print(f"  동일 개수: {cnt}  |  sentence_id: {ids}")
                print("-" * 60)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
