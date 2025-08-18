#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, json, sqlite3, datetime as dt, argparse, pathlib
from typing import Any, Dict, List, Optional

def ensure_dir(p: str) -> None:
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def to_date(ts: Optional[float]) -> Optional[str]:
    if ts is None: return None
    try:
        return dt.datetime.utcfromtimestamp(float(ts)).date().isoformat()
    except Exception:
        return None

def load_db(db_path: str) -> Dict[str, Any]:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    data = {}
    try:
        for name in ["conversions", "conversion_items", "posts"]:
            try:
                rows = con.execute(f"SELECT * FROM {name}").fetchall()
                data[name] = [dict(r) for r in rows]
            except Exception:
                data[name] = []
    finally:
        con.close()
    return data

def extract_variant(utm: Optional[str]) -> Optional[str]:
    if not utm or "-" not in utm: return None
    parts = utm.split("-")
    return parts[1].upper() if len(parts)>=2 and parts[1] else None

def num(x):
    try: return float(x)
    except Exception: return 0.0

def build_jsons(db_path: str, out_dir: str) -> None:
    ensure_dir(out_dir); ensure_dir(os.path.join(out_dir, "data"))
    raw = load_db(db_path)
    conv = raw["conversions"]
    items = raw["conversion_items"]
    posts = raw["posts"]

    for c in conv:
        if "purchaseTime" in c:
            c["purchase_date"] = to_date(c.get("purchaseTime"))
        if "utmContent" in c:
            c["variant"] = extract_variant(c.get("utmContent"))

    orders = len(conv)
    item_qty = sum(int(i.get("qty") or 0) for i in items) if items else 0
    # net
    if conv and "netCommission" in conv[0]:
        net = sum(num(c.get("netCommission")) for c in conv)
    else:
        net = sum(num(i.get("itemTotalCommission")) for i in items)
    avg = (net/orders) if orders>0 else 0.0

    json.dump({"orders":orders, "items":item_qty, "net_commission":net, "avg_per_order":avg},
              open(os.path.join(out_dir,"data","kpis.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # time series
    ts = {}
    for c in conv:
        d = c.get("purchase_date")
        if not d: continue
        val = num(c.get("netCommission")) if "netCommission" in c else 0.0
        ts[d] = ts.get(d, 0.0) + val
    ts_rows = [{"date": d, "net_commission": v} for d,v in sorted(ts.items())]
    json.dump(ts_rows, open(os.path.join(out_dir,"data","timeseries.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # A/B
    ab = {}
    for c in conv:
        v = c.get("variant"); 
        if not v: continue
        ab.setdefault(v, {"orders":0,"net_commission":0.0})
        ab[v]["orders"] += 1
        ab[v]["net_commission"] += num(c.get("netCommission")) if "netCommission" in c else 0.0
    ab_rows = [{"variant": v,"orders":d["orders"],"net_commission":d["net_commission"]} for v,d in ab.items()]
    json.dump(ab_rows, open(os.path.join(out_dir,"data","ab.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # categorias
    cats = {}
    for i in items:
        cat = i.get("globalCategoryLv1Name") or i.get("globalcategorylv1name") or i.get("category")
        if not cat: continue
        cats[cat] = cats.get(cat, 0.0) + num(i.get("itemTotalCommission"))
    cats_rows = [{"category":k, "net_commission":v} for k,v in sorted(cats.items(), key=lambda x:x[1], reverse=True)[:12]]
    json.dump(cats_rows, open(os.path.join(out_dir,"data","categories.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # lojas
    shops = {}
    for i in items:
        shop = i.get("shopName") or i.get("shop_name")
        if not shop: continue
        shops[shop] = shops.get(shop, 0.0) + num(i.get("itemTotalCommission"))
    shops_rows = [{"shop":k, "net_commission":v} for k,v in sorted(shops.items(), key=lambda x:x[1], reverse=True)[:12]]
    json.dump(shops_rows, open(os.path.join(out_dir,"data","shops.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # produtos
    prod = {}
    for i in items:
        key = (i.get("itemId") or i.get("item_id"), i.get("itemName"))
        if not key[0]: continue
        d = prod.setdefault(key, {"itemId": key[0], "itemName": key[1], "shopName": i.get("shopName"), "qty":0, "itemTotalCommission":0.0})
        d["qty"] += int(i.get("qty") or 0)
        d["itemTotalCommission"] += num(i.get("itemTotalCommission"))
    prod_rows = sorted(list(prod.values()), key=lambda x: x["itemTotalCommission"], reverse=True)[:20]
    json.dump(prod_rows, open(os.path.join(out_dir,"data","products.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # posts
    post_rows = []
    for p in posts[-200:]:
        date = p.get("posted_at") or p.get("created_at") or p.get("timestamp")
        post_rows.append({
            "date": str(date) if date is not None else None,
            "item_id": p.get("item_id") or p.get("itemId"),
            "category": p.get("category"),
            "variant": p.get("variant"),
            "cta": p.get("cta_used") or p.get("cta")
        })
    json.dump(post_rows, open(os.path.join(out_dir,"data","posts.json"),"w",encoding="utf-8"), ensure_ascii=False)

    # meta
    json.dump({"generated_at": dt.datetime.utcnow().isoformat()+"Z"},
              open(os.path.join(out_dir,"data","meta.json"),"w",encoding="utf-8"), ensure_ascii=False)

def maybe_ai_insights(out_dir: str) -> None:
    """Gera insights.json usando GEMINI_API_KEY se existir, ou um resumo heurístico."""
    import os, json
    data_dir = os.path.join(out_dir, "data")
    def load(n):
        p = os.path.join(data_dir, n)
        return json.load(open(p,"r",encoding="utf-8")) if os.path.exists(p) else None
    kpis = load("kpis.json") or {}
    ab = load("ab.json") or []
    cats = load("categories.json") or []
    shops = load("shops.json") or []
    products = load("products.json") or []

    summary = []
    summary.append(f"Pedidos: {kpis.get('orders',0)}, Itens: {kpis.get('items',0)}, Comissão líquida: R$ {kpis.get('net_commission',0):,.2f}".replace(",", "X").replace(".", ",").replace("X","."))
    if ab:
        top_ab = sorted(ab, key=lambda x: x.get('net_commission',0), reverse=True)[0]
        summary.append(f"Variante vencedora: {top_ab.get('variant')} ({top_ab.get('orders',0)} pedidos).")
    if cats:
        summary.append(f"Categoria destaque: {cats[0].get('category')} (R$ {cats[0].get('net_commission',0):,.2f})".replace(",", "X").replace(".", ",").replace("X","."))
    if shops:
        summary.append(f"Loja destaque: {shops[0].get('shop')} (R$ {shops[0].get('net_commission',0):,.2f})".replace(",", "X").replace(".", ",").replace("X","."))
    if products:
        summary.append(f"Produto destaque: {products[0].get('itemName')} (R$ {products[0].get('itemTotalCommission',0):,.2f}).".replace(",", "X").replace(".", ",").replace("X","."))

    payload = {"summary": " ".join(summary), "generated_by": "heuristic"}

    api_key = os.getenv("GEMINI_API_KEY","").strip()
    if api_key:
        try:
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel("gemini-1.5-flash")
            prompt = (
                "Resuma insights de performance (pt-BR): KPIs, A/B, top categorias/lojas/produtos e 3 ações de otimização.\n" +
                f"KPIs={json.dumps(kpis, ensure_ascii=False)} AB={json.dumps(ab, ensure_ascii=False)} " +
                f"CATS={json.dumps(cats[:5], ensure_ascii=False)} SHOPS={json.dumps(shops[:5], ensure_ascii=False)} PRODS={json.dumps(products[:5], ensure_ascii=False)}"
            )
            resp = model.generate_content(prompt)
            payload = {"summary": resp.text.strip(), "generated_by": "gemini"}
        except Exception:
            pass

    json.dump(payload, open(os.path.join(data_dir,"insights.json"),"w",encoding="utf-8"), ensure_ascii=False)

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH","data/bot.db"))
    ap.add_argument("--out", default="site")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    # copy static (assume files shipped with repo inside site_static/)
    here = os.path.dirname(__file__)
    for name in ("index.html","app.js","styles.css"):
        src = os.path.join(here, "site_static", name)
        dst = os.path.join(args.out, name)
        with open(src,"rb") as fsrc, open(dst,"wb") as fdst:
            fdst.write(fsrc.read())

    build_jsons(args.db, args.out)
    maybe_ai_insights(args.out)

if __name__ == "__main__":
    main()
