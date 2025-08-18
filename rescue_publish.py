# rescue_publish.py — garante atingir o número de posts com backfill e relaxamento
from __future__ import annotations
from typing import Callable, List, Dict, Any, Set, Tuple
import logging, time

log = logging.getLogger("rescue")

Product = Dict[str, Any]

def publish_with_rescue(
    ranked: List[Product],
    max_posts: int,
    can_repost: Callable[[int], bool],
    publish_func: Callable[[Product], bool],
    collect_relaxed: Callable[[], List[Product]] | None = None,
    id_key: str = "item_id",
    sleep_between: float = 0.6,
) -> Tuple[int, int]:
    """Tenta publicar até max_posts. Se pular por cooldown/erro, busca backfill do próprio ranking e,
    se necessário, ativa uma coleta relaxada (segundo passe).
    Retorna: (publicados, tentativas).
    """
    posted = 0
    tried = 0
    seen: Set[int] = set()
    idx = 0

    def _pick_next(pool: List[Product], start_idx: int) -> int:
        i = start_idx
        while i < len(pool):
            pid = int(pool[i].get(id_key) or 0)
            if pid and pid not in seen and can_repost(pid):
                return i
            i += 1
        return -1

    # 1) Passo inicial sobre o ranking
    while posted < max_posts:
        nxt = _pick_next(ranked, idx)
        if nxt == -1:
            break
        prod = ranked[nxt]
        pid = int(prod.get(id_key))
        seen.add(pid)
        tried += 1
        ok = publish_func(prod)
        if ok:
            posted += 1
            time.sleep(sleep_between)
        idx = nxt + 1

    # 2) Backfill pelo restante do ranking
    i = idx
    while posted < max_posts and i < len(ranked):
        pid = int(ranked[i].get(id_key) or 0)
        if pid and pid not in seen and can_repost(pid):
            tried += 1
            if publish_func(ranked[i]):
                posted += 1
                time.sleep(sleep_between)
            seen.add(pid)
        i += 1

    # 3) Coleta relaxada (segundo passe) se ainda faltar
    if posted < max_posts and collect_relaxed:
        log.warning("Ativando modo RESGATE: coletando mais itens com filtros relaxados...")
        extra = collect_relaxed()
        j = 0
        while posted < max_posts and j < len(extra):
            pid = int(extra[j].get(id_key) or 0)
            if pid and pid not in seen and can_repost(pid):
                tried += 1
                if publish_func(extra[j]):
                    posted += 1
                    time.sleep(sleep_between)
                seen.add(pid)
            j += 1

    return posted, tried
