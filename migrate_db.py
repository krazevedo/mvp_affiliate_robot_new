#!/usr/bin/env python3
"""migrate_db.py — aplica migrações no SQLite usando só Python (sem precisar do CLI sqlite3)

Uso:
  python migrate_db.py --db data/bot.db --sql sql_migrations/001_add_conversions.sql

Se o arquivo SQL não existir, cai no fallback e cria as tabelas chamando o ensure_schema
do conversions_sync.py.
"""
from __future__ import annotations
import argparse, os, sqlite3, sys

def run_sql_file(db_path: str, sql_path: str):
    if not os.path.exists(sql_path):
        raise FileNotFoundError(sql_path)
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.executescript(sql)
        con.commit()
        print(f"OK: migração aplicada a {db_path} via {sql_path}")
    finally:
        con.close()

def fallback_ensure_schema(db_path: str):
    # fallback: importa ensure_schema do conversions_sync.py
    import importlib.util
    spec = importlib.util.spec_from_file_location("conversions_sync", os.path.join(os.getcwd(), "conversions_sync.py"))
    if spec is None or spec.loader is None:
        raise RuntimeError("Não encontrei conversions_sync.py para fallback ensure_schema.")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    con = sqlite3.connect(db_path)
    try:
        mod.ensure_schema(con)  # type: ignore[attr-defined]
        print(f"OK: schema criado por fallback ensure_schema() em {db_path}")
    finally:
        con.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "data/bot.db"))
    ap.add_argument("--sql", default="sql_migrations/001_add_conversions.sql")
    args = ap.parse_args()

    try:
        run_sql_file(args.db, args.sql)
    except FileNotFoundError:
        print(f"Aviso: arquivo SQL {args.sql} não encontrado — usando fallback ensure_schema().")
        fallback_ensure_schema(args.db)

if __name__ == "__main__":
    main()
