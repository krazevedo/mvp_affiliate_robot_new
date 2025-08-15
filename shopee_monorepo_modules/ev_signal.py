# shopee_monorepo_modules/ev_signal.py
from __future__ import annotations
import sqlite3, time, math
from typing import Optional

def _sigmoid_like(x: float, k: float = 30.0) -> float:
    if x <= 0: return 0.0
    return 1.0 - math.exp(-x / max(1e-9, k))

def compute_ev_signal(db_path: str, *, item_id: int, product_name: str, shop_name: Optional[str], window_days: int = 28) -> float:
    cutoff = int(time.time()) - window_days * 86400
    cat = None
    item_ev = 0.0
    shop_ev = 0.0
    cat_ev = 0.0
    with sqlite3.connect(db_path) as con:
        cur = con.cursor()
        cur.execute("""
            SELECT COALESCE(SUM(ci.item_total_commission),0.0)
            FROM conversion_items ci
            JOIN conversions c ON c.conversion_id = ci.conversion_id
            WHERE (c.purchase_time IS NULL OR c.purchase_time >= ?)
              AND ci.item_id = ?
        """, (cutoff, item_id))
        row = cur.fetchone()
        if row and row[0] is not None:
            try: item_ev = float(row[0])
            except: item_ev = 0.0
        if shop_name:
            cur.execute("""
                SELECT COALESCE(SUM(ci.item_total_commission),0.0)
                FROM conversion_items ci
                JOIN conversions c ON c.conversion_id = ci.conversion_id
                WHERE (c.purchase_time IS NULL OR c.purchase_time >= ?)
                  AND ci.shop_name = ?
            """, (cutoff, shop_name))
            r2 = cur.fetchone()
            if r2 and r2[0] is not None:
                try: shop_ev = float(r2[0])
                except: shop_ev = 0.0
        cur.execute("""
            SELECT globalCategoryLv1Name, COUNT(*) AS n
            FROM conversion_items
            WHERE item_id = ?
            GROUP BY globalCategoryLv1Name
            ORDER BY n DESC LIMIT 1
        """, (item_id,))
        r3 = cur.fetchone()
        if r3 and r3[0]:
            cat = r3[0]
        if cat:
            cur.execute("""
                SELECT COALESCE(SUM(ci.item_total_commission),0.0)
                FROM conversion_items ci
                JOIN conversions c ON c.conversion_id = ci.conversion_id
                WHERE (c.purchase_time IS NULL OR c.purchase_time >= ?)
                  AND ci.globalCategoryLv1Name = ?
            """, (cutoff, cat))
            r4 = cur.fetchone()
            if r4 and r4[0] is not None:
                try: cat_ev = float(r4[0])
                except: cat_ev = 0.0

    s_item = _sigmoid_like(item_ev, 30.0)
    s_shop = _sigmoid_like(shop_ev, 80.0)
    s_cat  = _sigmoid_like(cat_ev, 150.0)
    return 0.6 * s_item + 0.3 * s_shop + 0.1 * s_cat
