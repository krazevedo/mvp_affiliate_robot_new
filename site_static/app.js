async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error("Erro ao carregar " + path);
  return await res.json();
}

function brl(v) {
  if (v === null || v === undefined) return "—";
  return "R$ " + Number(v).toLocaleString("pt-BR", {minimumFractionDigits: 2, maximumFractionDigits: 2});
}

function fmtNum(v) {
  if (v === null || v === undefined) return "—";
  return Number(v).toLocaleString("pt-BR");
}

function setText(id, text) {
  document.getElementById(id).textContent = text;
}

function renderTable(tableId, rows) {
  const tbody = document.querySelector(`#${tableId} tbody`);
  tbody.innerHTML = "";
  rows.forEach(r => {
    const tr = document.createElement("tr");
    r.forEach(c => {
      const td = document.createElement("td");
      td.textContent = c;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

async function main() {
  try {
    const [meta, kpis, ts, ab, cats, shops, products, posts] = await Promise.all([
      fetchJSON("./data/meta.json"),
      fetchJSON("./data/kpis.json"),
      fetchJSON("./data/timeseries.json"),
      fetchJSON("./data/ab.json"),
      fetchJSON("./data/categories.json"),
      fetchJSON("./data/shops.json"),
      fetchJSON("./data/products.json"),
      fetchJSON("./data/posts.json").catch(() => [])
    ]);

    if (meta?.generated_at) {
      document.getElementById("updated").textContent = "Atualizado em " + new Date(meta.generated_at).toLocaleString("pt-BR");
    }

    // KPIs
    setText("kpi-orders", fmtNum(kpis.orders));
    setText("kpi-items", fmtNum(kpis.items));
    setText("kpi-net", brl(kpis.net_commission));
    setText("kpi-avg", brl(kpis.avg_per_order));

    // Time series
    Plotly.newPlot("chart-timeseries", [{
      x: ts.map(d => d.date),
      y: ts.map(d => d.net_commission),
      type: "scatter",
      mode: "lines+markers",
      fill: "tozeroy"
    }], {margin:{t:10}});

    // A/B
    Plotly.newPlot("chart-ab-orders", [{
      x: ab.map(d => d.variant),
      y: ab.map(d => d.orders),
      type: "bar",
      text: ab.map(d => d.orders),
      textposition: "auto"
    }], {title: "Pedidos por Variante", margin:{t:30}});

    Plotly.newPlot("chart-ab-net", [{
      x: ab.map(d => d.variant),
      y: ab.map(d => d.net_commission),
      type: "bar",
      text: ab.map(d => "R$ " + Number(d.net_commission).toLocaleString("pt-BR", {minimumFractionDigits:2})),
      textposition: "auto"
    }], {title: "Comissão por Variante", margin:{t:30}});

    // Categorias & Lojas
    Plotly.newPlot("chart-cats", [{
      x: cats.map(d => d.category),
      y: cats.map(d => d.net_commission),
      type: "bar",
      text: cats.map(d => "R$ " + Number(d.net_commission).toLocaleString("pt-BR", {minimumFractionDigits:2})),
      textposition: "auto"
    }], {margin:{t:10}});

    Plotly.newPlot("chart-shops", [{
      x: shops.map(d => d.shop),
      y: shops.map(d => d.net_commission),
      type: "bar",
      text: shops.map(d => "R$ " + Number(d.net_commission).toLocaleString("pt-BR", {minimumFractionDigits:2})),
      textposition: "auto"
    }], {margin:{t:10}});

    // Top produtos
    renderTable("tbl-products", products.map(p => [
      p.itemName || p.item_id || p.itemId,
      p.shopName || "—",
      fmtNum(p.qty || 0),
      brl(p.itemTotalCommission || 0)
    ]));

    // Posts
    if (Array.isArray(posts) && posts.length) {
      renderTable("tbl-posts", posts.map(p => [
        p.date || "—", String(p.item_id || p.itemId || "—"),
        p.category || "—", p.variant || "—", p.cta || "—"
      ]));
    }

    // Insights da IA (se existir)
    try {
      const insights = await fetchJSON("./data/insights.json");
      if (insights && insights.summary) {
        document.getElementById("ai-insights").style.display = "block";
        document.getElementById("insights-text").textContent = insights.summary;
      }
    } catch(e) {
      // sem IA
    }

  } catch (e) {
    console.error(e);
    alert("Falha ao carregar dados do dashboard. Verifique se a publicação foi feita pelo GitHub Actions.");
  }
}

main();
