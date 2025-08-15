
"""
storage.py — Camada de persistência (SQLite) para o bot Shopee → Telegram.
"""
from __future__ import annotations
import sqlite3, pathlib
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

DB_PATH = "data/bot.db"

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS products (
  item_id INTEGER PRIMARY KEY, shop_id INTEGER, name TEXT, link TEXT, category TEXT,
  rating REAL, sales INTEGER, price_min REAL, price_max REAL, discount REAL,
  created_at TEXT, updated_at TEXT
);
CREATE TABLE IF NOT EXISTS prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER, price REAL, captured_at TEXT
);
CREATE TABLE IF NOT EXISTS posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, item_id INTEGER, variant TEXT, message_id TEXT, posted_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_prices_item ON prices(item_id);
CREATE INDEX IF NOT EXISTS idx_posts_item ON posts(item_id);
"""

def _utcnow_iso(): return datetime.utcnow().isoformat(timespec="seconds")

class Storage:
    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        pathlib.Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as con:
            con.executescript(SCHEMA)
    def _conn(self):
        con = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        con.row_factory = sqlite3.Row
        return con
    def upsert_product(self, prod: Dict[str, Any]) -> None:
        now = _utcnow_iso()
        with self._conn() as con:
            con.execute(
                """
                INSERT INTO products (item_id, shop_id, name, link, category, rating, sales, price_min, price_max, discount, created_at, updated_at)
                VALUES (:item_id, :shop_id, :name, :link, :category, :rating, :sales, :price_min, :price_max, :discount, :created_at, :updated_at)
                ON CONFLICT(item_id) DO UPDATE SET
                    shop_id=excluded.shop_id, name=excluded.name, link=excluded.link, category=excluded.category,
                    rating=excluded.rating, sales=excluded.sales, price_min=excluded.price_min, price_max=excluded.price_max,
                    discount=excluded.discount, updated_at=excluded.updated_at
                """,
                {
                    "item_id": prod.get("itemId") or prod.get("item_id"),
                    "shop_id": prod.get("shopId") or prod.get("shop_id"),
                    "name": prod.get("name") or prod.get("productName") or prod.get("itemName"),
                    "link": prod.get("productLink") or prod.get("link"),
                    "category": prod.get("category"),
                    "rating": float(prod.get("ratingStar") or prod.get("rating", 0)) if (prod.get("ratingStar") or prod.get("rating")) else None,
                    "sales": int(prod.get("sales", 0)) if prod.get("sales") is not None else None,
                    "price_min": _to_float(prod.get("priceMin")),
                    "price_max": _to_float(prod.get("priceMax")),
                    "discount": _to_float(prod.get("priceDiscountRate") or prod.get("discount")),
                    "created_at": now, "updated_at": now,
                },
            )
    def add_price_point(self, item_id: int, price: float, captured_at: Optional[str] = None) -> None:
        ts = captured_at or _utcnow_iso()
        with self._conn() as con:
            con.execute("INSERT INTO prices (item_id, price, captured_at) VALUES (?, ?, ?)", (item_id, price, ts))
    def latest_price(self, item_id: int) -> Optional[Tuple[float, str]]:
        with self._conn() as con:
            row = con.execute("SELECT price, captured_at FROM prices WHERE item_id=? ORDER BY captured_at DESC LIMIT 1", (item_id,)).fetchone()
        return (float(row["price"]), str(row["captured_at"])) if row else None
    def record_post(self, item_id: int, variant: str, message_id: str) -> None:
        with self._conn() as con:
            con.execute("INSERT INTO posts (item_id, variant, message_id, posted_at) VALUES (?, ?, ?, ?)", (item_id, variant, message_id, _utcnow_iso()))
    def last_posted_at(self, item_id: int) -> Optional[str]:
        with self._conn() as con:
            row = con.execute("SELECT posted_at FROM posts WHERE item_id=? ORDER BY posted_at DESC LIMIT 1", (item_id,)).fetchone()
        return str(row["posted_at"]) if row else None
    def can_repost(self, item_id: int, cooldown_days: int) -> bool:
        last = self.last_posted_at(item_id)
        if not last: return True
        try: from datetime import datetime as _dt; last_dt = _dt.fromisoformat(last)
        except Exception: return True
        from datetime import datetime as _dt; return _dt.utcnow() >= last_dt + timedelta(days=cooldown_days)

def _to_float(v):
    if v is None: return None
    try: return float(v)
    except Exception:
        try: return float(str(v).replace(",", ".").strip())
        except Exception: return None
