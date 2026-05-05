import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

try:
    import ijson
except ImportError:
    ijson = None  # type: ignore


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = ON;")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS sentences (
        sentence_id TEXT PRIMARY KEY,
        sentence_form TEXT NOT NULL
    );
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS predicates (
        predicate_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sentence_id TEXT NOT NULL,
        predicate_form TEXT NOT NULL,
        predicate_begin INTEGER NOT NULL,
        predicate_end INTEGER NOT NULL,
        predicate_lemma TEXT,
        sense_id INTEGER,
        FOREIGN KEY(sentence_id) REFERENCES sentences(sentence_id) ON DELETE CASCADE
    );
    """)
    # 기존 DB에 sense_id 컬럼이 없으면 추가
    cur = conn.execute("PRAGMA table_info(predicates);")
    columns = [row[1] for row in cur.fetchall()]
    if "sense_id" not in columns:
        conn.execute("ALTER TABLE predicates ADD COLUMN sense_id INTEGER;")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS arguments (
        argument_id INTEGER PRIMARY KEY AUTOINCREMENT,
        predicate_id INTEGER NOT NULL,
        arg_form TEXT NOT NULL,
        arg_label TEXT NOT NULL,
        arg_begin INTEGER NOT NULL,
        arg_end INTEGER NOT NULL,
        FOREIGN KEY(predicate_id) REFERENCES predicates(predicate_id) ON DELETE CASCADE
    );
    """)

    # 성능을 위한 인덱스
    conn.execute("CREATE INDEX IF NOT EXISTS idx_predicates_sentence ON predicates(sentence_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_arguments_predicate ON arguments(predicate_id);")
    conn.commit()


def safe_int(x: Any, default: int = -1) -> int:
    try:
        return int(x)
    except Exception:
        return default


def iter_sentences(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    # payload: { id, metadata, document:[{id, sentence:[...]}] }
    for doc in payload.get("document", []):
        for sent in doc.get("sentence", []):
            yield sent


def ingest_json_to_db(
    json_path: str,
    db_path: str = "srl.sqlite3",
    commit_every: int = 2000
) -> Tuple[int, int, int]:
    """
    Returns: (n_sentences, n_predicates, n_arguments)
    """
    json_path = str(json_path)
    db_path = str(db_path)

    payload = json.loads(Path(json_path).read_text(encoding="utf-8"))

    conn = sqlite3.connect(db_path)
    try:
        create_tables(conn)

        n_sent, n_pred, n_arg = 0, 0, 0
        batch_ops = 0

        for sent in iter_sentences(payload):
            sentence_id = sent.get("id")
            sentence_form = sent.get("form", "")

            if not sentence_id:
                continue

            # 문장 저장(중복 방지)
            conn.execute(
                "INSERT OR IGNORE INTO sentences(sentence_id, sentence_form) VALUES(?, ?);",
                (sentence_id, sentence_form)
            )
            n_sent += 1
            batch_ops += 1

            # SRL: list
            srl_list = sent.get("SRL") or []
            for frame in srl_list:
                pred = frame.get("predicate") or {}
                p_form = pred.get("form", "")
                p_begin = safe_int(pred.get("begin"), default=-1)
                p_end = safe_int(pred.get("end"), default=-1)
                p_lemma = pred.get("lemma")
                p_sense_id = pred.get("sense_id")
                if p_sense_id is not None:
                    p_sense_id = safe_int(p_sense_id, default=-1)
                    if p_sense_id == -1:
                        p_sense_id = None

                # predicate insert
                cur = conn.execute(
                    """
                    INSERT INTO predicates(sentence_id, predicate_form, predicate_begin, predicate_end, predicate_lemma, sense_id)
                    VALUES(?, ?, ?, ?, ?, ?);
                    """,
                    (sentence_id, p_form, p_begin, p_end, p_lemma, p_sense_id)
                )
                predicate_id = cur.lastrowid
                n_pred += 1
                batch_ops += 1

                # arguments insert
                args = frame.get("argument") or []
                for a in args:
                    a_form = a.get("form", "")
                    a_label = a.get("label", "")
                    a_begin = safe_int(a.get("begin"), default=-1)
                    a_end = safe_int(a.get("end"), default=-1)

                    conn.execute(
                        """
                        INSERT INTO arguments(predicate_id, arg_form, arg_label, arg_begin, arg_end)
                        VALUES(?, ?, ?, ?, ?);
                        """,
                        (predicate_id, a_form, a_label, a_begin, a_end)
                    )
                    n_arg += 1
                    batch_ops += 1

            if batch_ops >= commit_every:
                conn.commit()
                batch_ops = 0

        conn.commit()
        return n_sent, n_pred, n_arg

    finally:
        conn.close()


def build_wsd_sense_by_begin(nxls_path: str | Path) -> Dict[str, Dict[int, int]]:
    """
    NXLS JSON을 스트리밍으로 읽어, 문장 id별로 WSD 항목의 begin -> sense_id 맵을 만듭니다.
    WSD의 pos가 VV(동사), VA(형용사)인 항목만 포함합니다.
    동일 문장에서 begin이 겹치면 나중에 나온 WSD가 덮어씁니다.
    """
    nxls_path = Path(nxls_path)
    if ijson is None:
        raise RuntimeError("대용량 NXLS 처리에는 ijson이 필요합니다. pip install ijson")

    lookup: Dict[str, Dict[int, int]] = {}
    with open(nxls_path, "rb") as f:
        for doc in ijson.items(f, "document.item"):
            for sent in doc.get("sentence", []):
                sid = sent.get("id")
                if not sid:
                    continue
                if sid not in lookup:
                    lookup[sid] = {}
                for w in sent.get("WSD") or []:
                    pos = w.get("pos")
                    if pos not in ("VV", "VA"):
                        continue
                    begin = w.get("begin")
                    sense_id = w.get("sense_id")
                    if begin is not None and sense_id is not None:
                        lookup[sid][int(begin)] = int(sense_id)
    return lookup


def ingest_nxsr_nxls_to_db(
    nxsr_path: str | Path,
    nxls_path: str | Path,
    db_path: str | Path,
    *,
    replace_db: bool = True,
    commit_every: int = 2000,
) -> Tuple[int, int, int, int]:
    """
    NXSR의 SRL(predicate/argument)을 그대로 DB에 넣고,
    같은 문장 id에 대해 NXLS의 WSD에서 begin이 predicate.begin과 같으면 sense_id를 할당합니다.
    (WSD는 pos가 VV, VA인 항목만 sense_id 맵에 포함)

    Returns: (n_sentences, n_predicates, n_arguments, n_predicates_with_sense_id)
    """
    nxsr_path = Path(nxsr_path)
    nxls_path = Path(nxls_path)
    db_path = Path(db_path)

    if not nxsr_path.exists():
        raise FileNotFoundError(f"NXSR 파일 없음: {nxsr_path}")
    if not nxls_path.exists():
        raise FileNotFoundError(f"NXLS 파일 없음: {nxls_path}")
    if ijson is None:
        raise RuntimeError("대용량 NXSR/NXLS 처리에는 ijson이 필요합니다. pip install ijson")

    if replace_db and db_path.exists():
        db_path.unlink()

    wsd_by_sentence = build_wsd_sense_by_begin(nxls_path)

    conn = sqlite3.connect(db_path)
    try:
        create_tables(conn)

        n_sent, n_pred, n_arg, n_with_sense = 0, 0, 0, 0
        batch_ops = 0

        with open(nxsr_path, "rb") as f:
            for doc in ijson.items(f, "document.item"):
                for sent in doc.get("sentence", []):
                    sentence_id = sent.get("id")
                    sentence_form = sent.get("form", "")

                    if not sentence_id:
                        continue

                    conn.execute(
                        "INSERT OR IGNORE INTO sentences(sentence_id, sentence_form) VALUES(?, ?);",
                        (sentence_id, sentence_form),
                    )
                    n_sent += 1
                    batch_ops += 1

                    wsd_begin_map = wsd_by_sentence.get(sentence_id, {})

                    srl_list = sent.get("SRL") or []
                    for frame in srl_list:
                        pred = frame.get("predicate") or {}
                        p_form = pred.get("form", "")
                        p_begin = safe_int(pred.get("begin"), default=-1)
                        p_end = safe_int(pred.get("end"), default=-1)
                        p_lemma = pred.get("lemma")

                        p_sense_id: Optional[int] = None
                        if p_begin >= 0 and p_begin in wsd_begin_map:
                            p_sense_id = wsd_begin_map[p_begin]
                            n_with_sense += 1

                        cur = conn.execute(
                            """
                            INSERT INTO predicates(sentence_id, predicate_form, predicate_begin, predicate_end, predicate_lemma, sense_id)
                            VALUES(?, ?, ?, ?, ?, ?);
                            """,
                            (sentence_id, p_form, p_begin, p_end, p_lemma, p_sense_id),
                        )
                        predicate_id = cur.lastrowid
                        n_pred += 1
                        batch_ops += 1

                        args = frame.get("argument") or []
                        for a in args:
                            a_form = a.get("form", "")
                            a_label = a.get("label", "")
                            a_begin = safe_int(a.get("begin"), default=-1)
                            a_end = safe_int(a.get("end"), default=-1)

                            conn.execute(
                                """
                                INSERT INTO arguments(predicate_id, arg_form, arg_label, arg_begin, arg_end)
                                VALUES(?, ?, ?, ?, ?);
                                """,
                                (predicate_id, a_form, a_label, a_begin, a_end),
                            )
                            n_arg += 1
                            batch_ops += 1

                    if batch_ops >= commit_every:
                        conn.commit()
                        batch_ops = 0

        conn.commit()
        return n_sent, n_pred, n_arg, n_with_sense
    finally:
        conn.close()


def example_queries(db_path: str = "srl.sqlite3") -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # 문장별 predicate/argument 조인 예시
        rows = conn.execute("""
        SELECT
            s.sentence_id,
            s.sentence_form,
            p.predicate_id,
            p.predicate_form,
            p.predicate_begin,
            p.predicate_end,
            p.predicate_lemma,
            p.sense_id,
            a.arg_form,
            a.arg_label,
            a.arg_begin,
            a.arg_end
        FROM sentences s
        JOIN predicates p ON s.sentence_id = p.sentence_id
        LEFT JOIN arguments a ON p.predicate_id = a.predicate_id
        ORDER BY s.sentence_id, p.predicate_id, a.argument_id;
        """).fetchmany(20)

        for r in rows:
            print(dict(r))
    finally:
        conn.close()


if __name__ == "__main__":
    base = Path(__file__).resolve().parent
    nxsr = base / "data" / "NXSR1902111171.json"
    nxls = base / "data" / "NXLS2002104060.json"
    db_path = base / "srlDB_with_senseid.sqlite3"

    # 기존: 단일 JSON 전체 로드 (소용량 전용)
    # n_sent, n_pred, n_arg = ingest_json_to_db(nxsr, db_path=db_path)

    n_sent, n_pred, n_arg, n_sense = ingest_nxsr_nxls_to_db(nxsr, nxls, db_path, replace_db=True)
    print(
        f"Done. sentences={n_sent}, predicates={n_pred}, arguments={n_arg}, "
        f"predicates_with_sense_id={n_sense}"
    )
    # example_queries(str(db_path))
