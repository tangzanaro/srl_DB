"""
srlDB_with_senseid.sqlite3용 predicate 수정 도구 (GUI).
lemma로 검색 → 문장 표시 → predicate_lemma, sense_id 수정.
"""
import os
import sys

# macOS에서 시스템/CommandLineTools Python의 Tk가 "macOS 26 or later required" 오류 시
# Anaconda Python으로 자동 재실행
if sys.platform == "darwin":
    exe = sys.executable
    exe_real = os.path.realpath(exe)
    is_system_python = (
        exe == "/usr/bin/python3"
        or exe.startswith("/usr/bin/python")
        or "CommandLineTools" in exe
        or "CommandLineTools" in exe_real
        or "Python3.framework" in exe_real
    )
    alt = "/opt/anaconda3/bin/python"
    if is_system_python and os.path.isfile(alt) and os.access(alt, os.X_OK):
        os.execv(alt, [alt] + sys.argv)

import json
import queue
import sqlite3
import threading
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
from pathlib import Path
from typing import List, Optional, Any, Tuple, Dict

# (sense_no, definition, examples). examples: 최대 2개, [{"example": str, "source": str}]
UrimalsamEntry = Tuple[str, str, List[Dict[str, str]]]

DB_PATH = Path(__file__).resolve().parent / "srlDB_with_senseid.sqlite3"
URIMALSAM_DIR = Path(__file__).resolve().parent / "urimalsam"
MAX_SEARCH = 5000000


def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB 파일을 찾을 수 없습니다: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def search_by_lemma(conn: sqlite3.Connection, lemma: str) -> List[sqlite3.Row]:
    q = """
    SELECT p.predicate_id, p.sentence_id, s.sentence_form,
           p.predicate_form, p.predicate_lemma, p.sense_id,
           p.predicate_begin, p.predicate_end
    FROM predicates p
    JOIN sentences s ON p.sentence_id = s.sentence_id
    WHERE p.predicate_lemma = ? OR p.predicate_lemma LIKE ? || '%'
    ORDER BY p.predicate_id
    LIMIT ?
    """
    cur = conn.execute(q, (lemma.strip(), lemma.strip(), MAX_SEARCH))
    return cur.fetchall()


def get_predicate_row(conn: sqlite3.Connection, predicate_id: int) -> Optional[sqlite3.Row]:
    q = """
    SELECT p.predicate_id, p.sentence_id, s.sentence_form,
           p.predicate_form, p.predicate_lemma, p.sense_id,
           p.predicate_begin, p.predicate_end
    FROM predicates p
    JOIN sentences s ON p.sentence_id = s.sentence_id
    WHERE p.predicate_id = ?
    """
    cur = conn.execute(q, (predicate_id,))
    return cur.fetchone()


def update_predicate(
    conn: sqlite3.Connection,
    predicate_id: int,
    predicate_lemma: Optional[str],
    sense_id: Optional[int],
) -> None:
    conn.execute(
        "UPDATE predicates SET predicate_lemma = ?, sense_id = ? WHERE predicate_id = ?",
        (predicate_lemma, sense_id, predicate_id),
    )
    conn.commit()


def normalize_word_for_search(word: str) -> str:
    """검색 시 기호(^, 공백, 문장부호 등)를 제거한 형태. 기호 없이도 검색 가능하게 함."""
    return "".join(c for c in word if c.isalnum())


def build_urimalsam_index_all(json_paths: List[Path]) -> Dict[str, List[UrimalsamEntry]]:
    """우리말샘 JSON 전체에서 word -> [(sense_no, definition, examples), ...] 인덱스 구축.
    examples: example_info 중 최대 2개, 각각 {"example": str, "source": str}. 없으면 [].
    """
    index: Dict[str, List[UrimalsamEntry]] = {}
    for path in json_paths:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        items = data.get("channel", {}).get("item", [])
        for it in items:
            wordinfo = it.get("wordinfo") or {}
            senseinfo = it.get("senseinfo") or {}
            word = (wordinfo.get("word") or "").strip()
            if not word:
                continue
            sense_no = senseinfo.get("sense_no") or ""
            definition = senseinfo.get("definition") or ""
            raw_examples = senseinfo.get("example_info") or []
            examples: List[Dict[str, str]] = []
            for ex in raw_examples[:2]:
                if isinstance(ex, dict):
                    e = (ex.get("example") or "").strip()
                    s = (ex.get("source") or "").strip()
                    if e:
                        examples.append({"example": e, "source": s})
            if word not in index:
                index[word] = []
            index[word].append((sense_no, definition, examples))
    return index


def search_word_in_index(
    index: Dict[str, List[UrimalsamEntry]],
    word: str,
    max_results: int = 300,
) -> List[UrimalsamEntry]:
    """구축된 인덱스에서 word 검색. 기호 없이 검색 가능 (정규화 비교)."""
    word = word.strip()
    norm_query = normalize_word_for_search(word)
    matches: List[UrimalsamEntry] = []
    if word in index:
        matches = index[word][:max_results]
    elif norm_query:
        for w, lst in index.items():
            norm_w = normalize_word_for_search(w)
            if norm_w == norm_query or norm_w.startswith(norm_query) or norm_query in norm_w:
                matches.extend(lst)
                if len(matches) >= max_results:
                    matches = matches[:max_results]
                    break
        if not matches:
            for w, lst in index.items():
                if w.startswith(word) or word in w:
                    matches.extend(lst)
                    if len(matches) >= max_results:
                        matches = matches[:max_results]
                        break
    return matches


class PredicateEditorApp:
    def __init__(self) -> None:
        self.conn = get_conn()
        self._rows: List[Any] = []  # 검색 결과 (sqlite3.Row)
        self._selected_predicate_id: Optional[int] = None
        # 우리말샘: 인덱스 한 번 구축 후 메모리에서 검색 (속도 개선)
        self._urimalsam_index: Dict[str, List[UrimalsamEntry]] = {}
        self._urimalsam_index_ready = False
        self._urimalsam_building = False
        self._urimalsam_queue: queue.Queue = queue.Queue()
        self._urimalsam_pending_word: Optional[str] = None
        self.root = tk.Tk()
        self.root.title("Predicate 수정 도구 (srlDB_with_senseid)")
        self.root.geometry("900x650")
        self.root.minsize(700, 500)

        self._build_ui()

    def _build_ui(self) -> None:
        # 검색 영역 (lemma로 검색, 엔터=검색)
        search_frame = ttk.Frame(self.root, padding=8)
        search_frame.pack(fill=tk.X)
        ttk.Label(search_frame, text="lemma:").pack(side=tk.LEFT, padx=(0, 4))
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=25)
        self.search_entry.pack(side=tk.LEFT, padx=4)
        self.search_entry.bind("<Return>", lambda e: self._on_search())
        self.search_btn = ttk.Button(search_frame, text="검색", command=self._on_search)
        self.search_btn.pack(side=tk.LEFT, padx=4)
        ttk.Label(search_frame, text="(Enter=검색 · ↑↓=행이동 · sense_id에서 Enter=저장 후 다음)").pack(side=tk.LEFT, padx=4)
        self.result_label = ttk.Label(search_frame, text="")
        self.result_label.pack(side=tk.LEFT, padx=8)

        # 좌우 분할: 왼쪽 = 테이블+상세, 오른쪽 = 우리말샘
        paned = tk.PanedWindow(self.root, orient=tk.HORIZONTAL, sashrelief=tk.RAISED, sashwidth=6)
        paned.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        left_frame = ttk.Frame(paned)
        paned.add(left_frame, minsize=380)

        # 결과 테이블 (sentence_preview 컬럼 없음)
        table_frame = ttk.Frame(left_frame, padding=(8, 0))
        table_frame.pack(fill=tk.BOTH, expand=True)
        columns = ("predicate_id", "predicate_form", "lemma", "sense_id")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=12, selectmode="browse")
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100)
        scroll_y = ttk.Scrollbar(table_frame)
        scroll_x = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        self.tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)
        scroll_y.configure(command=self.tree.yview)
        scroll_x.configure(command=self.tree.xview)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # 상세 / 수정 영역
        detail_frame = ttk.LabelFrame(left_frame, text="선택한 predicate 상세 및 수정", padding=8)
        detail_frame.pack(fill=tk.BOTH, expand=False, padx=8, pady=8)

        ttk.Label(detail_frame, text="문장(sentence_form) — 굵은 부분이 predicate_form:").pack(anchor=tk.W)
        self.sentence_text = scrolledtext.ScrolledText(detail_frame, height=10, wrap=tk.WORD, state=tk.NORMAL)
        self.sentence_text.pack(fill=tk.X, pady=(0, 8))
        # predicate_form 하이라이트용 태그 (배경색 + 굵게)
        self.sentence_text.tag_configure("predicate_highlight", background="#a0ffff", font=("TkDefaultFont", 10, "bold"))
        self.sentence_text.tag_configure("persistent_highlight", background="#cce5ff")
        self.sentence_text.bind("<ButtonRelease-1>", self._on_sentence_select)
        self.sentence_text.bind("<KeyRelease>", self._on_sentence_select)
        self.sentence_text.bind("<Key>", self._sentence_text_key)

        edit_row = ttk.Frame(detail_frame)
        edit_row.pack(fill=tk.X, pady=4)
        ttk.Label(edit_row, text="predicate_lemma:").pack(side=tk.LEFT, padx=(0, 4))
        self.lemma_var = tk.StringVar()
        self.lemma_entry = ttk.Entry(edit_row, textvariable=self.lemma_var, width=30)
        self.lemma_entry.pack(side=tk.LEFT, padx=4)
        ttk.Label(edit_row, text="sense_id:").pack(side=tk.LEFT, padx=(12, 4))
        self.sense_var = tk.StringVar()
        self.sense_entry = ttk.Entry(edit_row, textvariable=self.sense_var, width=12)
        self.sense_entry.pack(side=tk.LEFT, padx=4)
        self.sense_entry.bind("<Return>", lambda e: self._save_and_next(e))
        ttk.Label(edit_row, text="(Enter=저장 후 다음)").pack(side=tk.LEFT, padx=2)
        self.save_btn = ttk.Button(edit_row, text="저장", command=self._on_save)
        self.save_btn.pack(side=tk.LEFT, padx=8)
        self.save_next_btn = ttk.Button(edit_row, text="저장 후 다음 (Enter)", command=self._save_and_next_click)
        self.save_next_btn.pack(side=tk.LEFT, padx=4)

        # 단축키: 검색 Ctrl+G, 저장 Ctrl+S, 저장 후 다음 Ctrl+Enter
        self.root.bind_all("<Control-g>", self._on_search_key)
        self.root.bind_all("<Control-G>", self._on_search_key)
        self.root.bind_all("<Control-s>", self._on_save_key)
        self.root.bind_all("<Control-S>", self._on_save_key)
        self.root.bind_all("<Control-Return>", lambda e: self._save_and_next(e))

        # 탭 순서: 검색란 → 결과 목록(tree) → lemma → sense_id → 검색란 (위·아래로 결과 행 이동)
        self._tab_order = [self.search_entry, self.tree, self.lemma_entry, self.sense_entry]
        for w in self._tab_order:
            w.bind("<Tab>", self._focus_next)
            w.bind("<Shift-Tab>", self._focus_prev)

        self.status_var = tk.StringVar(
            value="검색(Ctrl+G) → ↑↓로 행 선택 → sense_id 입력 후 Enter: 저장 후 다음 행 · Ctrl+S: 저장만"
        )

        # 오른쪽: 우리말샘 사전 패널
        urimalsam_frame = ttk.LabelFrame(paned, text="우리말샘 사전", padding=6)
        paned.add(urimalsam_frame, minsize=280)
        urimalsam_row1 = ttk.Frame(urimalsam_frame)
        urimalsam_row1.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(urimalsam_row1, text="word:").pack(side=tk.LEFT, padx=(0, 4))
        self.urimalsam_word_var = tk.StringVar()
        self.urimalsam_word_entry = ttk.Entry(urimalsam_row1, textvariable=self.urimalsam_word_var, width=16)
        self.urimalsam_word_entry.pack(side=tk.LEFT, padx=4)
        self.urimalsam_word_entry.bind("<Return>", lambda e: self._urimalsam_search())
        ttk.Button(urimalsam_row1, text="검색", command=self._urimalsam_search).pack(side=tk.LEFT, padx=4)
        ttk.Label(urimalsam_row1, text="").pack(side=tk.LEFT, padx=4)
        self.urimalsam_text = scrolledtext.ScrolledText(urimalsam_frame, height=8, wrap=tk.WORD, state=tk.NORMAL)
        self.urimalsam_text.tag_configure("sense_no", font=("TkDefaultFont", 10, "bold"))
        self.urimalsam_text.tag_configure("example", foreground="DarkSlateGray")
        self.urimalsam_text.tag_configure("example_source", font=("TkDefaultFont", 9), foreground="Gray")
        self.urimalsam_text.tag_configure("persistent_highlight", background="#cce5ff")  # 드래그 선택 영역 강조 유지
        self.urimalsam_text.bind("<ButtonRelease-1>", self._urimalsam_on_select)
        self.urimalsam_text.bind("<KeyRelease>", self._urimalsam_on_select)
        self.urimalsam_text.bind("<Key>", self._urimalsam_text_key)
        self.urimalsam_text.pack(fill=tk.BOTH, expand=True)

        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X, padx=4, pady=4)

    def _on_sentence_select(self, event: Optional[tk.Event] = None) -> None:
        """문장 영역: 드래그한 구간을 persistent_highlight로 유지. 행 선택 바꿀 때까지 유지."""
        self.root.after(0, lambda: self._apply_persistent_highlight(self.sentence_text))

    def _urimalsam_on_select(self, event: Optional[tk.Event] = None) -> None:
        """우리말샘: 드래그한 구간을 persistent_highlight로 유지. 새 검색 전까지 유지."""
        self.root.after(0, lambda: self._apply_persistent_highlight(self.urimalsam_text))

    def _apply_persistent_highlight(self, text_widget: tk.Text) -> None:
        """텍스트 위젯의 현재 선택 영역에 persistent_highlight 태그 추가. 이전 강조는 유지(여러 구간 강조 가능)."""
        try:
            first = text_widget.index(tk.SEL_FIRST)
            last = text_widget.index(tk.SEL_LAST)
        except tk.TclError:
            return
        if not first or not last:
            return
        text_widget.tag_add("persistent_highlight", first, last)

    def _sentence_text_key(self, event: tk.Event) -> Optional[str]:
        """문장 영역: 읽기 전용. 문자 입력·삭제 차단."""
        if event.keysym in ("BackSpace", "Delete", "Return", "KP_Enter"):
            return "break"
        if event.char and event.char.isprintable():
            return "break"
        return None

    def _urimalsam_text_key(self, event: tk.Event) -> Optional[str]:
        """우리말샘 텍스트 영역: 읽기 전용. 문자 입력·삭제 차단, 선택/복사/이동/Tab만 허용."""
        if event.keysym in ("BackSpace", "Delete", "Return", "KP_Enter"):
            return "break"
        if event.char and event.char.isprintable():
            return "break"
        return None

    def _get_sense_id_completion_str(self) -> str:
        """현재 검색 결과(_rows) 중 sense_id가 입력된 개수. '15건 / 100건' 형태. 없으면 빈 문자열."""
        if not self._rows:
            return ""
        total = len(self._rows)
        completed = sum(
            1
            for r in self._rows
            if r["sense_id"] is not None and str(r["sense_id"]).strip() != ""
        )
        return f"  입력 완료: {completed}건 / {total}건"

    def _urimalsam_show_results(
        self,
        word: str,
        matches: List[UrimalsamEntry],
        total_files: int = 0,
    ) -> None:
        """우리말샘 검색 결과를 텍스트 영역에 표시. definition 아래에 example_info 최대 2개 출력."""
        max_results = 300
        self.urimalsam_text.config(state=tk.NORMAL)
        self.urimalsam_text.delete(1.0, tk.END)
        if not matches:
            msg = f"'{word}'에 대한 항목이 없습니다."
            if total_files:
                msg += f" (인덱스: {len(self._urimalsam_index)}개 단어)"
            self.urimalsam_text.insert(tk.END, msg)
        else:
            for sense_no, definition, examples in matches:
                self.urimalsam_text.insert(tk.END, f"[sense_no] {sense_no}\n", "sense_no")
                self.urimalsam_text.insert(tk.END, f"[definition] {definition}\n", "definition")
                for ex in examples:
                    self.urimalsam_text.insert(tk.END, f"  [example] {ex.get('example', '')}\n", "example")
                    if ex.get("source"):
                        self.urimalsam_text.insert(tk.END, f"    (출처: {ex['source']})\n", "example_source")
                self.urimalsam_text.insert(tk.END, "\n", "definition")
            if len(matches) >= max_results:
                self.urimalsam_text.insert(tk.END, f"... (최대 {max_results}건까지 표시)\n", "definition")
        # NORMAL 유지 → 사용자가 드래그로 선택 후 persistent_highlight 유지 가능
        completion = self._get_sense_id_completion_str()
        self.status_var.set(f"우리말샘: '{word}' 검색 결과 {len(matches)}건.{completion}")

    def _urimalsam_poll_build(self) -> None:
        """백그라운드 인덱스 구축 완료 여부 확인 (메인 스레드에서만 호출)."""
        try:
            msg = self._urimalsam_queue.get_nowait()
        except queue.Empty:
            self.root.after(300, self._urimalsam_poll_build)
            return
        if msg[0] == "done":
            self._urimalsam_index = msg[1]
            self._urimalsam_index_ready = True
            self._urimalsam_building = False
            pending = self._urimalsam_pending_word
            self._urimalsam_pending_word = None
            self.status_var.set(f"우리말샘: 인덱스 구축 완료 (단어 {len(self._urimalsam_index)}개).")
            if pending:
                matches = search_word_in_index(self._urimalsam_index, pending)
                self._urimalsam_show_results(pending, matches)
        elif msg[0] == "progress":
            self.status_var.set(msg[1])

    def _urimalsam_build_index_worker(self, json_paths: List[Path]) -> None:
        """백그라운드 스레드: 전체 인덱스 구축 후 큐에 넣음."""
        try:
            idx = build_urimalsam_index_all(json_paths)
            self._urimalsam_queue.put(("done", idx))
        except Exception as e:
            self._urimalsam_queue.put(("done", {}))  # 실패 시 빈 인덱스

    def _urimalsam_search(self) -> None:
        """word 검색. 인덱스가 있으면 즉시 검색, 없으면 첫 검색 시 한 번만 구축 후 검색."""
        word = self.urimalsam_word_var.get().strip()
        if not word:
            self.urimalsam_text.config(state=tk.NORMAL)
            self.urimalsam_text.delete(1.0, tk.END)
            self.status_var.set("우리말샘: word를 입력하세요.")
            return
        if not URIMALSAM_DIR.exists():
            self.status_var.set("우리말샘: urimalsam 폴더가 없습니다.")
            messagebox.showwarning("우리말샘", "urimalsam 폴더를 찾을 수 없습니다.")
            return
        json_files = sorted(URIMALSAM_DIR.glob("*.json"), key=lambda p: p.name)
        if not json_files:
            self.status_var.set("우리말샘: urimalsam 폴더에 JSON 파일이 없습니다.")
            return

        if self._urimalsam_index_ready:
            # 인덱스 있음 → 즉시 검색
            matches = search_word_in_index(self._urimalsam_index, word)
            self._urimalsam_show_results(word, matches)
            return

        if self._urimalsam_building:
            self.status_var.set("우리말샘: 인덱스 구축 중... 완료 후 자동 검색됩니다.")
            self._urimalsam_pending_word = word
            return

        # 첫 검색: 백그라운드에서 인덱스 구축
        self._urimalsam_building = True
        self._urimalsam_pending_word = word
        self.status_var.set("우리말샘: 인덱스 구축 중... (첫 검색 시 한 번만, 완료 후 자동 검색)")
        t = threading.Thread(target=self._urimalsam_build_index_worker, args=(json_files,), daemon=True)
        t.start()
        self.root.after(300, self._urimalsam_poll_build)

    def _on_search_key(self, event: tk.Event) -> str:
        """Ctrl+G 단축키: 검색 실행 후 키 입력 차단 (검색란/sense_id 입력 중에도 동작)."""
        self._on_search()
        return "break"

    def _on_save_key(self, event: tk.Event) -> str:
        """Ctrl+S 단축키: 저장 후 키 입력 차단 (검색란/sense_id 입력 중에도 동작)."""
        self._on_save()
        return "break"

    def _focus_next(self, event: tk.Event) -> str:
        focus = self.root.focus_get()
        try:
            i = self._tab_order.index(focus)
            self._tab_order[(i + 1) % len(self._tab_order)].focus_set()
        except ValueError:
            pass
        return "break"

    def _focus_prev(self, event: tk.Event) -> str:
        focus = self.root.focus_get()
        try:
            i = self._tab_order.index(focus)
            self._tab_order[(i - 1) % len(self._tab_order)].focus_set()
        except ValueError:
            pass
        return "break"

    def _on_search(self) -> None:
        lemma = self.search_var.get().strip()
        if not lemma:
            self.status_var.set("lemma를 입력하세요.")
            return
        try:
            self._rows = search_by_lemma(self.conn, lemma)
        except Exception as e:
            messagebox.showerror("검색 오류", str(e))
            return
        self._refresh_table()
        self.result_label.config(text=f"검색 결과 {len(self._rows)}건")
        completion = self._get_sense_id_completion_str()
        self.status_var.set(f"검색 완료: {len(self._rows)}건.{completion}  ↑↓로 행 이동, 탭으로 입력란 이동.")
        # 결과가 있으면 첫 행 선택 후 sense_id에 포커스 → 바로 입력 후 Enter로 연속 작업
        if self._rows:
            first = self.tree.get_children()
            if first:
                self.tree.selection_set(first[0])
                self.tree.see(first[0])
                self.root.after(50, self._focus_sense_id)  # 선택 반영 후 sense_id 포커스

    def _refresh_table(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        for r in self._rows:
            pid = r["predicate_id"]
            form = (r["predicate_form"] or "")[:20]
            lemma = (r["predicate_lemma"] or "")[:20]
            sid = r["sense_id"] if r["sense_id"] is not None else ""
            self.tree.insert("", tk.END, values=(pid, form, lemma, sid))

    def _on_select(self, event: tk.Event) -> None:
        sel = self.tree.selection()
        if not sel:
            return
        item = self.tree.item(sel[0])
        vals = item["values"]
        if not vals:
            return
        predicate_id = int(vals[0])
        self._selected_predicate_id = predicate_id
        row = get_predicate_row(self.conn, predicate_id)
        if not row:
            return
        sent = row["sentence_form"] or ""
        begin = row["predicate_begin"]
        end = row["predicate_end"]

        self.sentence_text.config(state=tk.NORMAL)
        self.sentence_text.delete(1.0, tk.END)
        self.sentence_text.insert(tk.END, sent)
        # predicate_form 구간 하이라이트 (predicate_begin, predicate_end; 0-based 문자 위치)
        if begin is not None and end is not None and 0 <= begin < end <= len(sent):
            start_idx = f"1.{begin}"
            end_idx = f"1.{end}"
            self.sentence_text.tag_remove("predicate_highlight", "1.0", tk.END)
            self.sentence_text.tag_add("predicate_highlight", start_idx, end_idx)
        else:
            self.sentence_text.tag_remove("predicate_highlight", "1.0", tk.END)
        # NORMAL 유지 → 드래그로 선택 영역 강조 가능
        self.lemma_var.set(row["predicate_lemma"] or "")
        self.sense_var.set("" if row["sense_id"] is None else str(row["sense_id"]))
        self.sense_entry.select_range(0, tk.END)
        self.root.after(0, self._focus_sense_id)  # 선택 시 sense_id로 포커스 → 바로 입력 가능
        completion = self._get_sense_id_completion_str()
        self.status_var.set(f"선택: predicate_id={predicate_id}. sense_id 입력 후 Enter=저장 후 다음.{completion}")

    def _focus_sense_id(self) -> None:
        """sense_id 입력란으로 포커스 (바로 입력 가능)."""
        self.sense_entry.focus_set()

    def _save_and_next_click(self) -> None:
        self._save_and_next(None)

    def _save_and_next(self, event: Optional[tk.Event] = None) -> Optional[str]:
        """저장 후 다음 행으로 이동하고 sense_id에 포커스. Enter로 연속 작업용."""
        if self._selected_predicate_id is None or not self._rows:
            return "break" if event else None
        self._on_save()
        # 다음 행 찾기
        children = self.tree.get_children()
        sel = self.tree.selection()
        if not children or not sel:
            completion = self._get_sense_id_completion_str()
            self.status_var.set(f"저장했습니다.{completion}")
            return "break" if event else None
        try:
            idx = children.index(sel[0])
        except ValueError:
            completion = self._get_sense_id_completion_str()
            self.status_var.set(f"저장했습니다.{completion}")
            return "break" if event else None
        next_idx = idx + 1
        completion = self._get_sense_id_completion_str()
        if next_idx < len(children):
            next_item = children[next_idx]
            self.tree.selection_set(next_item)
            self.tree.see(next_item)
            self.root.after(0, self._focus_sense_id)
            self.status_var.set(f"저장 후 다음 행으로 이동. sense_id 입력 후 Enter.{completion}")
        else:
            self.status_var.set(f"저장했습니다. (마지막 행){completion}")
        return "break" if event else None

    def _on_save(self) -> None:
        if self._selected_predicate_id is None:
            self.status_var.set("먼저 목록에서 행을 선택하세요.")
            return
        predicate_id = self._selected_predicate_id
        new_lemma = self.lemma_var.get().strip() or None
        sense_str = self.sense_var.get().strip()
        if sense_str.lower() in ("null", "none", "-", ""):
            new_sense: Optional[int] = None
        else:
            try:
                new_sense = int(sense_str)
            except ValueError:
                messagebox.showwarning("입력 오류", "sense_id는 정수 또는 빈칸(NULL)으로 입력하세요.")
                return
        try:
            update_predicate(self.conn, predicate_id, new_lemma, new_sense)
        except Exception as e:
            messagebox.showerror("저장 오류", str(e))
            return
        # 테이블/상세 갱신: 현재 검색 결과에서 해당 행 찾아 갱신
        for i, r in enumerate(self._rows):
            if r["predicate_id"] == predicate_id:
                self._rows[i] = get_predicate_row(self.conn, predicate_id)
                break
        self._refresh_table()
        self.lemma_var.set(new_lemma or "")
        self.sense_var.set("" if new_sense is None else str(new_sense))
        completion = self._get_sense_id_completion_str()
        self.status_var.set(f"저장했습니다.{completion}")
        # 저장한 행 다시 선택
        for item in self.tree.get_children():
            if self.tree.item(item)["values"] and self.tree.item(item)["values"][0] == predicate_id:
                self.tree.selection_set(item)
                self.tree.see(item)
                break

    def run(self) -> None:
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()

    def _on_close(self) -> None:
        self.conn.close()
        self.root.destroy()


def main() -> None:
    try:
        app = PredicateEditorApp()
        app.run()
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)


if __name__ == "__main__":
    main()
