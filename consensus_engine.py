# -*- coding: utf-8 -*-
"""
539 全網數據聚合器（Data Aggregator）
ConsensusEngine：整合樂透雲、FB 社團、YouTube 報牌等來源，產出共識得分與過度對齊號碼清單。
"""

import json
import re
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional


@dataclass
class SourceResult:
    """單一來源的抓取結果"""
    name: str
    numbers: List[int]
    raw_count: int  # 該來源中此號碼出現次數（若為合併結果則為 1）


class ConsensusEngine:
    """
    共識引擎：從多來源聚合號碼，統計頻率並轉為共識得分 (0-100)，
    輸出當日被大眾「過度對齊」的號碼清單 JSON。
    """

    MIN_NUM = 1
    MAX_NUM = 39
    DEFAULT_OVER_ALIGN_THRESHOLD = 60  # 共識得分 >= 此值視為過度對齊

    def __init__(self, over_align_threshold: int = 60):
        self.over_align_threshold = over_align_threshold
        self._all_sources: List[SourceResult] = []

    # ========== 來源一：樂透雲（模擬）— 基本面 ==========
    def fetch_lotto_cloud(self) -> SourceResult:
        """
        模擬從樂透雲抓取「當日熱門推薦號碼」與「網友預測榜」。
        實作時可改為真實 HTTP 請求 + 解析 HTML/API。
        """
        # 模擬：熱門統計 + 預測榜各回傳一組，合併後當作多筆出現
        hot_stats = self._simulate_lotto_cloud_hot()
        prediction_rank = self._simulate_lotto_cloud_predictions()
        numbers = hot_stats + prediction_rank
        return SourceResult(name="樂透雲_熱門與預測榜", numbers=numbers, raw_count=len(numbers))

    def _simulate_lotto_cloud_hot(self) -> List[int]:
        """模擬熱門號碼統計（可替換成真實抓取）"""
        # 範例：模擬當日熱門 5 碼 + 重複權重
        return [3, 7, 11, 18, 22, 22, 29, 33, 35]

    def _simulate_lotto_cloud_predictions(self) -> List[int]:
        """模擬網友預測榜（可替換成真實抓取）"""
        return [5, 11, 18, 27, 31, 35, 39]

    # ========== 來源二：FB 社團（模擬）— 情緒面 ==========
    def fetch_fb_539_group(self) -> SourceResult:
        """
        模擬從 FB 539 大型公開社團抓取「被留言提到最多次」的號碼。
        實作時可改為 FB Graph API 或爬蟲（需遵守平台規範）。
        """
        # 模擬：多篇貼文留言中提取的號碼（重複愈多 = 情緒愈熱）
        numbers = self._simulate_fb_mentions()
        return SourceResult(name="FB539社團_留言熱門", numbers=numbers, raw_count=len(numbers))

    def _simulate_fb_mentions(self) -> List[int]:
        """模擬留言中被提到最多次的號碼（可替換成真實抓取）"""
        return [7, 11, 11, 18, 22, 22, 22, 29, 33, 35, 35, 39]

    # ========== 來源三：YouTube 報牌（模擬）— 引導面 ==========
    def fetch_youtube_titles(self) -> SourceResult:
        """
        模擬從 YouTube 報牌大師頻道（九爺、阿圖等）標題用正則提取數字。
        實作時可改為 YouTube Data API 搜尋標題後正則提取。
        """
        titles = self._simulate_youtube_titles()
        numbers = self._extract_numbers_from_text(titles)
        return SourceResult(name="YouTube報牌_標題數字", numbers=numbers, raw_count=len(numbers))

    def _simulate_youtube_titles(self) -> List[str]:
        """模擬影片標題（可替換成 API 回傳）"""
        return [
            "539 今天 03 07 18 22 35 必出",
            "阿圖 推薦 11 29 33 穩",
            "九爺 報牌 05 18 27 31",
            "今日 熱門 07 22 35",
        ]

    def _extract_numbers_from_text(self, texts: List[str]) -> List[int]:
        """用正則從多段文字中提取 1–39 的數字"""
        pattern = re.compile(r"\b([1-9]|[1-3][0-9])\b")
        numbers = []
        for text in texts:
            for m in pattern.findall(text):
                n = int(m)
                if self.MIN_NUM <= n <= self.MAX_NUM:
                    numbers.append(n)
        return numbers

    # ========== 運算：頻率統計 → 共識得分 ==========
    def add_source(self, result: SourceResult) -> None:
        """加入單一來源結果，供後續聚合"""
        self._all_sources.append(result)

    def run_sources(self) -> None:
        """依序執行三個來源（模擬抓取）並加入引擎"""
        self._all_sources.clear()
        self.add_source(self.fetch_lotto_cloud())
        self.add_source(self.fetch_fb_539_group())
        self.add_source(self.fetch_youtube_titles())

    def frequency_count(self) -> Counter:
        """統計每個號碼 (1–39) 的出現頻率"""
        counter: Counter = Counter()
        for src in self._all_sources:
            for n in src.numbers:
                if self.MIN_NUM <= n <= self.MAX_NUM:
                    counter[n] += 1
        return counter

    def to_consensus_scores(self, counter: Optional[Counter] = None) -> Dict[int, float]:
        """
        將頻率轉為共識得分 (0–100)。
        公式：該號碼頻率 / 全體最大頻率 * 100，若無資料則 0。
        """
        if counter is None:
            counter = self.frequency_count()
        if not counter:
            return {n: 0.0 for n in range(self.MIN_NUM, self.MAX_NUM + 1)}
        max_count = max(counter.values())
        return {
            n: round(100.0 * counter.get(n, 0) / max_count, 1)
            for n in range(self.MIN_NUM, self.MAX_NUM + 1)
        }

    def get_over_aligned_numbers(
        self,
        scores: Optional[Dict[int, float]] = None,
        threshold: Optional[int] = None,
    ) -> List[Dict]:
        """
        取得當日被大眾「過度對齊」的號碼清單（紅燈：過熱）。
        回傳格式： [ {"number": 7, "consensus_score": 85.0}, ... ]
        """
        if scores is None:
            scores = self.to_consensus_scores()
        th = threshold if threshold is not None else self.over_align_threshold
        return [
            {"number": n, "consensus_score": scores[n]}
            for n in range(self.MIN_NUM, self.MAX_NUM + 1)
            if scores[n] >= th
        ]

    def get_cold_calm_numbers(
        self,
        scores: Optional[Dict[int, float]] = None,
        score_threshold: int = 30,
        omission_days: Optional[Dict[int, int]] = None,
        omission_min_days: int = 5,
    ) -> List[Dict]:
        """
        取得「冷靜」號碼：全網沒人提（低共識）+ 歷史遺漏高（可選）。
        實作時 omission_days 可改為真實開獎歷史計算。回傳綠燈推薦。
        """
        if scores is None:
            scores = self.to_consensus_scores()
        if omission_days is None:
            omission_days = self._mock_omission_days()
        return [
            {"number": n, "consensus_score": scores[n], "omission_days": omission_days.get(n, 0)}
            for n in range(self.MIN_NUM, self.MAX_NUM + 1)
            if scores[n] <= score_threshold and omission_days.get(n, 0) >= omission_min_days
        ]

    def _mock_omission_days(self) -> Dict[int, int]:
        """模擬歷史遺漏天數（實作時改為真實開獎紀錄計算）"""
        import random
        r = random.Random(42)
        return {n: r.randint(0, 15) for n in range(self.MIN_NUM, self.MAX_NUM + 1)}

    def get_master_backtest(self) -> List[Dict]:
        """
        模擬大師勝率回測（過去一週準確率）。
        實作時改為真實紀錄：比對大師當日推薦 vs 實際開獎。
        """
        return [
            {"name": "九爺", "hit_rate_pct": 18.2, "sample_size": 7, "trend": "down"},
            {"name": "阿圖", "hit_rate_pct": 22.1, "sample_size": 7, "trend": "up"},
            {"name": "其他指標頻道", "hit_rate_pct": 15.0, "sample_size": 7, "trend": "flat"},
        ]

    # ========== 輸出：生成 JSON 檔案 ==========
    def build_output(
        self,
        over_aligned: List[Dict],
        scores: Dict[int, float],
        counter: Counter,
        cold_calm: Optional[List[Dict]] = None,
        master_backtest: Optional[List[Dict]] = None,
    ) -> Dict:
        """組裝要寫入 JSON 的完整結構（含儀表板用欄位）"""
        if cold_calm is None:
            cold_calm = self.get_cold_calm_numbers(scores=scores)
        if master_backtest is None:
            master_backtest = self.get_master_backtest()
        return {
            "generated_at": datetime.now().isoformat(),
            "description": "539 全網數據聚合 — 當日共識得分與過度對齊號碼",
            "sources": [{"name": s.name, "sample_size": len(s.numbers)} for s in self._all_sources],
            "frequency_count": dict(counter),
            "consensus_scores": scores,
            "over_aligned_numbers": sorted(over_aligned, key=lambda x: -x["consensus_score"]),
            "over_aligned_threshold": self.over_align_threshold,
            "cold_calm_numbers": sorted(cold_calm, key=lambda x: -x.get("omission_days", 0)),
            "master_backtest": master_backtest,
        }

    def run(self) -> Dict:
        """執行完整流程：抓取 → 統計 → 得分 → 紅燈/綠燈/大師回測"""
        self.run_sources()
        counter = self.frequency_count()
        scores = self.to_consensus_scores(counter)
        over_aligned = self.get_over_aligned_numbers(scores)
        cold_calm = self.get_cold_calm_numbers(scores=scores)
        master_backtest = self.get_master_backtest()
        return self.build_output(over_aligned, scores, counter, cold_calm=cold_calm, master_backtest=master_backtest)

    def run_and_save_json(self, filepath: Optional[str] = None) -> str:
        """
        執行聚合並將結果寫入 JSON 檔案。
        若未傳 filepath，則使用預設檔名（含當天日期）。
        """
        data = self.run()
        if filepath is None:
            filepath = f"539_consensus_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
        path = Path(filepath)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(path.resolve())


# ========== 方便直接執行與測試 ==========
if __name__ == "__main__":
    engine = ConsensusEngine(over_align_threshold=60)
    data = engine.run()
    ts_path = Path(f"539_consensus_{datetime.now().strftime('%Y%m%d_%H%M')}.json")
    ts_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"已寫入: {ts_path.resolve()}")
    latest_path = Path("539_consensus_latest.json")
    latest_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"儀表板用: {latest_path.resolve()}")
    print("紅燈（大多數人推薦）:", [x["number"] for x in data["over_aligned_numbers"]])
    print("綠燈（沒什麼人提）:", [x["number"] for x in data["cold_calm_numbers"]])
