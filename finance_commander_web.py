import html
import json
import mimetypes
import os
import re
import subprocess
import uuid
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd

from finance_commander import (
    PBI_DIR,
    RAW_DIR,
    cloud_ingest,
    confirm_staged_import,
    get_latest_report_path,
    get_runtime_params,
    get_staged_summary,
    get_status,
    route_command,
    set_runtime_params,
    stage_dataframe,
    standardize_with_mapping,
    suggest_mapping,
)

HOST = os.getenv("R2R_HOST", ("0.0.0.0" if os.getenv("PORT") else "127.0.0.1"))
PORT = int(os.getenv("PORT", os.getenv("R2R_PORT", "8787")))
BASE_DIR = Path(os.getenv("R2R_BASE_DIR", Path(__file__).resolve().parent))
UPLOAD_DIR = RAW_DIR / "web_uploads"
PENDING_DIR = RAW_DIR / "pending_mapping"
ASSET_DIR = BASE_DIR / "assets"

ACTION_MAP = {
    "start_close": "开始月度结账",
    "generate_report": "生成本月初步报表",
    "check_status": "查看审批状态",
    "publish": "审批通过，更新看板",
}

LAST_RESULT = "欢迎来到旺财。请先导入数据，上传可分多批次，完成后点击“确认完成导入”。"
LAST_COMMAND = ""
LAST_TIME = ""
PENDING_CONFIRM: dict = {}


def get_alert_summary(threshold_val: float) -> tuple[bool, str]:
    ds = PBI_DIR / "pbi_dataset.csv"
    if not ds.exists():
        return False, "暂无预警数据"
    try:
        df = pd.read_csv(ds)
    except Exception:
        return False, "预警数据读取失败"

    if "revenue_mom_pct" not in df.columns:
        return False, "未检测到收入环比字段"

    threshold = float(threshold_val) / 100.0
    mom = pd.to_numeric(df["revenue_mom_pct"], errors="coerce").dropna()
    if mom.empty:
        return False, "暂无可计算的收入环比"

    diff = mom.abs() - threshold
    if (diff > 0).any():
        max_hit = float(mom.abs().max() * 100)
        return True, f"发现波动 {max_hit:.2f}% 超过阈值 {threshold_val:.2f}%"

    latest = float(mom.iloc[-1] * 100)
    return False, f"当前未触发预警（最新环比 {latest:.2f}%）"


def parse_multipart(body: bytes, content_type: str):
    m = re.search(r"boundary=(.+)", content_type)
    if not m:
        return {}, {}

    boundary = m.group(1).strip().strip('"').encode("utf-8")
    marker = b"--" + boundary
    parts = body.split(marker)

    fields = {}
    files = {}

    for part in parts:
        part = part.strip()
        if not part or part == b"--":
            continue

        if b"\r\n\r\n" not in part:
            continue

        head, content = part.split(b"\r\n\r\n", 1)
        content = content.rstrip(b"\r\n")
        headers = head.decode("utf-8", errors="ignore")

        disp = ""
        for line in headers.split("\r\n"):
            if line.lower().startswith("content-disposition"):
                disp = line
                break

        name_m = re.search(r'name="([^"]+)"', disp)
        file_m = re.search(r'filename="([^"]*)"', disp)
        if not name_m:
            continue

        name = name_m.group(1)
        if file_m and file_m.group(1):
            files[name] = {
                "filename": Path(file_m.group(1)).name,
                "content": content,
            }
        else:
            fields[name] = content.decode("utf-8", errors="ignore")

    return fields, files


def _confirm_panel(columns: list[str], sugg: dict, token: str) -> str:
    def options_html(selected: str | None) -> str:
        opts = ['<option value="">(未识别)</option>']
        for c in columns:
            sel = " selected" if selected and c == selected else ""
            opts.append(f'<option value="{html.escape(c)}"{sel}>{html.escape(c)}</option>')
        return "".join(opts)

    def select(name: str, selected: str | None) -> str:
        return f"""
        <label>{name}</label>
        <select name="{name}">
          {options_html(selected)}
        </select>
        <input type="hidden" name="{name}_suggest" value="{html.escape(selected or '')}" />
        """

    msg = f"我识别到你的‘月份/日期’可能在【{sugg.get('fiscal_date') or '未识别'}】列，‘销售收入/金额’可能在【{sugg.get('amount') or '未识别'}】列。请确认后加入暂存区。"

    return f"""
    <div class=\"glass card col-12\">
      <h2>🤖 智能字段确认</h2>
      <div class=\"result\" style=\"max-height:140px;\">{html.escape(msg)}</div>
      <form method=\"post\" action=\"/confirm-mapping\" class=\"map-grid\">
        <input type=\"hidden\" name=\"token\" value=\"{html.escape(token)}\" />
        {select('fiscal_date', sugg.get('fiscal_date'))}
        {select('amount', sugg.get('amount'))}
        {select('account_name', sugg.get('account_name'))}
        {select('ticker', sugg.get('ticker'))}
        <label>默认股票代码（无对应列时使用）</label>
        <input type=\"text\" name=\"default_ticker\" value=\"MANUAL\" />
        <div style=\"display:flex; gap:8px; margin-top:8px;\">
          <button class=\"btn\" type=\"submit\" name=\"decision\" value=\"confirm\">确认并加入暂存</button>
          <button class=\"btn btn-secondary\" type=\"submit\" name=\"decision\" value=\"cancel\">取消</button>
        </div>
      </form>
    </div>
    """

def render_page() -> str:
    params = get_runtime_params()
    threshold_val = float(params.get("threshold_val", 10.0))
    status = get_status()
    staged = get_staged_summary()
    has_alert, alert_msg = get_alert_summary(threshold_val)
    latest_report = get_latest_report_path()
    report_path = str(latest_report) if latest_report else "暂无"
    pbi_pbids = PBI_DIR / "R2R_Local_Dataset.pbids"
    pbi_csv = PBI_DIR / "pbi_dataset.csv"
    pbi_path_obj = pbi_pbids if pbi_pbids.exists() else (pbi_csv if pbi_csv.exists() else None)
    pbi_path = str(pbi_path_obj) if pbi_path_obj else "暂无"

    status_color = "#2e7d32" if status["effective"] == "Approved" else "#ef6c00"
    alert_color = "#c62828" if has_alert else "#2e7d32"

    buttons = "".join(
        [
            f"""
            <form method="post" action="/run" class="action-form">
              <input type="hidden" name="action" value="{k}" />
              <button type="submit">{v}</button>
            </form>
            """
            for k, v in ACTION_MAP.items()
        ]
    )

    confirm_html = ""
    if PENDING_CONFIRM:
        confirm_html = _confirm_panel(
            PENDING_CONFIRM.get("columns", []),
            PENDING_CONFIRM.get("suggestion", {}),
            PENDING_CONFIRM.get("token", ""),
        )

    path_link = "/preview-report" if latest_report else "#"
    folder_link = "/open-report-folder" if latest_report else "#"

    pbi_link = "/preview-powerbi" if pbi_path_obj else "#"
    pbi_folder_link = "/open-pbi-folder" if pbi_path_obj else "#"

    return f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>旺财 - 超强财务报表数据自动化控制台</title>
  <style>
    :root {{
      --bg1:#fff4c7;
      --bg2:#ffe7a8;
      --bg3:#f8d98f;
      --card:#fffaf0;
      --line:#e6cf9e;
      --text:#3b2a11;
      --muted:#7a5b2e;
      --primary:#b86a00;
      --primary-hover:#9f5900;
      --secondary:#a48b5f;
      --link:#1f5fbf;
    }}
    * {{ box-sizing:border-box; }}
    body {{
      margin:0; min-height:100vh;
      font-family:"Microsoft YaHei","PingFang SC",sans-serif;
      color:var(--text);
      background: linear-gradient(135deg,var(--bg1) 0%, var(--bg2) 48%, var(--bg3) 100%);
    }}
    .wrap {{ max-width: 1160px; margin: 16px auto; padding: 0 12px; }}
    .glass {{ background: rgba(255,250,240,0.95); border:1px solid var(--line); border-radius:16px; box-shadow:0 8px 20px rgba(126,82,13,0.10); }}
    .hero {{ padding:16px 18px; display:grid; grid-template-columns: 1.3fr 0.9fr 0.8fr; gap:12px; align-items:center; }}
    h1 {{ margin:0 0 6px; font-size:52px; line-height:1; letter-spacing:1px; }}
    .sub {{ margin:0; color:var(--muted); font-weight:700; font-size:22px; }}
    .hero-image-wrap {{ display:flex; justify-content:center; align-items:center; justify-self:center; }}
    .hero-image {{ width:100%; max-width:300px; height:110px; object-fit:cover; border-radius:12px; border:1px solid #d9bf89; }}
    .chip {{ display:inline-block; padding:5px 10px; border-radius:999px; color:#fff; font-size:12px; font-weight:700; margin-bottom:8px; }}

    .grid {{ display:grid; gap:12px; margin-top:12px; grid-template-columns: repeat(12, 1fr); }}
    .card {{ padding:14px; grid-column: span 12; }}
    .col-6 {{ grid-column: span 6; }}
    .col-12 {{ grid-column: span 12; }}
    .full-height {{ min-height: 240px; }}

    @media (max-width: 980px) {{
      .hero {{ grid-template-columns: 1fr; }}
      .col-6 {{ grid-column: span 12; }}
      .full-height{{min-height:auto;}}
    }}

    h2 {{ margin:0 0 10px; font-size:26px; }}
    .kv {{ line-height:1.7; color:#4a3719; font-size:17px; }}

    .path-line {{ margin-top:6px; }}
    .path-ellipsis {{
      display:block; max-width:100%; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
      border:1px dashed #d5ba86; border-radius:8px; padding:6px 8px; background:#fffdf8;
    }}
    .path-link {{ color:var(--link); text-decoration:underline; font-weight:700; }}
    .path-actions {{ display:flex; gap:10px; margin-top:6px; font-size:14px; flex-wrap:wrap; }}

    .actions {{ display:grid; grid-template-columns: repeat(2, minmax(180px,1fr)); gap:10px; align-content:start; }}
    .flow-card {{ display:flex; flex-direction:column; }}
    .flow-note {{ margin-top:auto; padding-top:16px; }}
    .action-form button, .btn {{
      width:100%; border:none; border-radius:12px; padding:11px; font-size:16px; font-weight:700;
      background:var(--primary); color:#fff; cursor:pointer;
    }}
    .action-form button:hover, .btn:hover {{ background:var(--primary-hover); }}
    .btn-secondary {{ background:var(--secondary); }}

    .desc-block {{ font-size:15px; line-height:1.6; color:#5a4420; margin-bottom:8px; }}
    .import-row {{ display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:center; }}
    .import-row input[type='text'], .import-row input[type='file'], .map-grid input, .map-grid select {{
      width:100%; border:1px solid #d3b885; border-radius:10px; padding:10px; background:#fffdf7; font-size:15px;
    }}

    .map-grid {{ display:grid; grid-template-columns: repeat(2, minmax(180px, 1fr)); gap:10px; margin-top:8px; }}
    .map-grid label {{ font-size:13px; color:#6b5431; font-weight:700; }}

    .result {{
      max-height:120px; overflow:auto; background:#fffdf8; border:1px dashed #d1b076; border-radius:10px;
      padding:9px; line-height:1.35; font-size:14px;
    }}
    .meta {{ margin-top:6px; color:var(--muted); font-size:13px; }}
    .footer-note {{ color:var(--muted); font-size:12px; margin-top:8px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="glass hero">
      <div>
        <h1>💰 旺财</h1>
        <p class="sub">超强财务报表数据自动化控制台</p>
      </div>
      <div class="hero-image-wrap">
        <img class="hero-image" src="/asset/hero_cat.jpg" alt="旺财头图" />
      </div>
      <div>
        <div><span class="chip" style="background:{status_color};">审批状态：{html.escape(status['effective'])}</span></div>
        <div><span class="chip" style="background:{alert_color};">预警状态：{html.escape(alert_msg)}</span></div>
      </div>
    </div>

    <div class="grid">
      <div class="glass card col-12">
        <h2>🧾 执行结果</h2>
        <div class="result" style="color:{'#c62828' if has_alert else '#3b2a11'};">{html.escape(LAST_RESULT)}</div>
        <div class="meta">最后动作：{html.escape(LAST_COMMAND or '-')} ｜ 执行时间：{html.escape(LAST_TIME or '-')}</div>
      </div>

      <div class="glass card col-6 full-height">
        <h2>📌 当前状态</h2>
        <div class="kv">
          审批状态：<strong>{html.escape(status['db_status'])}</strong>
          <div class="path-line">导入暂存：<strong>{staged['batch_count']} 批 / {staged['row_count']} 行</strong></div>
          <div class="path-line">初步报表路径：</div>
          <a class="path-link path-ellipsis" title="{html.escape(report_path)}" href="{path_link}">{html.escape(report_path)}</a>
          <div class="path-actions">
            <a class="path-link" href="{folder_link}">打开报表文件夹</a>
            <a class="path-link" href="/preview-report?page=1">网页预览Excel</a>
          </div>
          <div class="path-line">看板可视化路径：</div>
          <a class="path-link path-ellipsis" title="{html.escape(pbi_path)}" href="{pbi_link}">{html.escape(pbi_path)}</a>
          <div class="path-actions">
            <a class="path-link" href="{pbi_folder_link}">打开看板文件夹</a>
            <a class="path-link" href="/preview-powerbi?page=1">网页预览Power BI</a>
          </div>
        </div>
      </div>

      <div class="glass card col-6 full-height flow-card">
        <h2>⚡ 一键流程操作</h2>
        <div class="desc-block">
          推荐顺序：确认完成导入 → 开始月度结账 → 确认波动参数并计算 → 生成本月初步报表 → 查看审批状态 → 审批通过，更新看板
        </div>
        <div class="actions">
          <form method="post" action="/confirm-import" class="action-form">
            <button type="submit">① 确认完成导入</button>
          </form>
          <form method="post" action="/run" class="action-form">
            <input type="hidden" name="action" value="start_close" />
            <button type="submit">② 开始月度结账</button>
          </form>
          <form method="post" action="/api/v1/confirm_approval" class="action-form">
            <input type="hidden" name="redirect" value="1" />
            <button type="submit">③ 确认波动参数并计算</button>
          </form>
          <form method="post" action="/run" class="action-form">
            <input type="hidden" name="action" value="generate_report" />
            <button type="submit">④ 生成本月初步报表</button>
          </form>
          <form method="post" action="/run" class="action-form">
            <input type="hidden" name="action" value="check_status" />
            <button type="submit">⑤ 查看审批状态</button>
          </form>
          <form method="post" action="/run" class="action-form">
            <input type="hidden" name="action" value="publish" />
            <button type="submit">⑥ 审批通过，更新看板</button>
          </form>
        </div>
        <p class="footer-note flow-note">请按上述 ① 到 ⑥ 顺序执行，状态结果会在上方“执行结果”实时回显。</p>
      </div>

      <div class="glass card col-6">
        <h2>一、云端取数</h2>
        <div class="desc-block">输入ERP 账套名称或 API 地址</div>
        <form method="post" action="/cloud-ingest" class="import-row">
          <input type="text" name="cloud_input" placeholder="输入 ERP账套名 / https://api.example.com/data.json" required />
          <button class="btn" type="submit">执行云端取数</button>
        </form>
      </div>

      <div class="glass card col-6">
        <h2>二、本地表单导入</h2>
        <div class="desc-block">支持多批次上传 CSV/Excel，上传后仅进入暂存区，不直接入库</div>
        <form method="post" action="/local-import" enctype="multipart/form-data" class="import-row">
          <input type="file" name="file" accept=".csv,.xlsx,.xls" required />
          <button class="btn" type="submit">上传并加入暂存</button>
        </form>
      </div>

      <div class="glass card col-12">
        <h2>三、动态预算参数与预警阈值</h2>
        <form method="post" action="/set-runtime-params" class="map-grid">
          <label>去年平均增长率（默认参考，如 0.08）</label>
          <input type="text" name="last_year_avg_growth" value="{html.escape(str(params['last_year_avg_growth']))}" />
          <label>期望增长率（如 0.12）</label>
          <input type="text" name="expected_growth" value="{html.escape(str(params['expected_growth']))}" />
          <label>预算金额（可留空）</label>
          <input type="text" name="budget_amount" value="{html.escape(str(params['budget_amount']))}" />
          <label>波动阈值 threshold_val（百分数，如 15）</label>
          <input type="text" name="threshold_val" value="{html.escape(str(params['threshold_val']))}" />
          <label>营业收入权重（totalRevenue）</label>
          <input type="text" name="weight_totalRevenue" value="{html.escape(str(params['weight_totalRevenue']))}" />
          <label>销售费用权重（sellingGeneralAndAdministrative）</label>
          <input type="text" name="weight_sellingGeneralAndAdministrative" value="{html.escape(str(params['weight_sellingGeneralAndAdministrative']))}" />
          <div style="display:flex; gap:8px; margin-top:8px; grid-column: span 2;">
            <button class="btn" type="submit">保存参数并实时生效</button>
          </div>
        </form>
      </div>

      {confirm_html}
    </div>
  </div>
</body>
</html>
"""


class CommanderHandler(BaseHTTPRequestHandler):
    def _send_html(self, body: str, status_code: int = 200):
        data = body.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _send_json(self, payload: dict, status_code: int = 200):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _set_result(self, command: str, result: str):
        global LAST_RESULT, LAST_COMMAND, LAST_TIME
        LAST_COMMAND = command
        LAST_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        LAST_RESULT = result

    def _redirect(self, location: str = "/"):
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        self.end_headers()

    def _send_bytes(self, data: bytes, content_type: str):
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=60")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            self._send_html(render_page())
            return

        if path == "/asset/hero_cat.jpg":
            img = ASSET_DIR / "hero_cat.jpg"
            if not img.exists():
                self.send_error(HTTPStatus.NOT_FOUND, "图片不存在")
                return
            data = img.read_bytes()
            ctype, _ = mimetypes.guess_type(str(img))
            self._send_bytes(data, ctype or "image/jpeg")
            return

        if path == "/open-report-folder":
            report = get_latest_report_path()
            if not report:
                self._set_result("打开报表文件夹", "未找到报表文件。")
                self._redirect("/")
                return
            try:
                if os.name == "nt":
                    os.startfile(str(report.parent))  # type: ignore[attr-defined]
                else:
                    subprocess.Popen(["xdg-open", str(report.parent)])
                self._set_result("打开报表文件夹", f"已尝试打开文件夹：{report.parent}")
            except Exception as e:
                self._set_result("打开报表文件夹", f"打开失败：{e}")
            self._redirect("/")
            return

        if path == "/open-pbi-folder":
            pbi_pbids = PBI_DIR / "R2R_Local_Dataset.pbids"
            pbi_csv = PBI_DIR / "pbi_dataset.csv"
            target = pbi_pbids if pbi_pbids.exists() else (pbi_csv if pbi_csv.exists() else None)
            if not target:
                self._set_result("打开看板文件夹", "未找到看板文件。")
                self._redirect("/")
                return
            try:
                if os.name == "nt":
                    os.startfile(str(target.parent))  # type: ignore[attr-defined]
                else:
                    subprocess.Popen(["xdg-open", str(target.parent)])
                self._set_result("打开看板文件夹", f"已尝试打开文件夹：{target.parent}")
            except Exception as e:
                self._set_result("打开看板文件夹", f"打开失败：{e}")
            self._redirect("/")
            return

        if path == "/preview-report":
            report = get_latest_report_path()
            if not report or not report.exists():
                self._send_html("<h3>未找到可预览的报表文件。</h3>")
                return
            try:
                page_no = int((query.get("page", ["1"])[0] or "1").strip())
                page_no = max(1, page_no)
                xls = pd.ExcelFile(report)
                sheet_names = xls.sheet_names[:6]
                sheet_index = min(page_no - 1, len(sheet_names) - 1)
                sheet = sheet_names[sheet_index]
                df = pd.read_excel(report, sheet_name=sheet).head(100)
                for col in ["环比%", "同比%", "revenue_mom_pct", "yoy_pct", "mom_pct"]:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

                nav = "".join(
                    [
                        f"<a href='/preview-report?page={idx + 1}' style='margin-right:8px;'>{html.escape(name)}</a>"
                        for idx, name in enumerate(sheet_names)
                    ]
                )
                page = f"""
                <html><head><meta charset='utf-8'><title>报表预览</title>
                <style>
                body{{font-family:Microsoft YaHei,sans-serif;padding:16px;background:#fff9e8;color:#3b2a11;}}
                .tb{{border-collapse:collapse;width:100%;margin-bottom:16px;background:#fff;}}
                .tb th,.tb td{{border:1px solid #e6cf9e;padding:6px 8px;font-size:13px;}}
                .tb th{{background:#fff1cc;}}
                a{{color:#1f5fbf;}}
                </style></head><body>
                <p><a href="/">返回旺财控制台</a></p>
                <h2>Excel 报表预览：{html.escape(str(report.name))}</h2>
                <div>{nav}</div>
                <h3>{html.escape(sheet)}</h3>
                {df.to_html(index=False, border=0, classes='tb')}
                </body></html>
                """
                self._send_html(page)
            except Exception as e:
                self._send_html(f"<h3>预览失败：{html.escape(str(e))}</h3><p><a href='/'>返回</a></p>")
            return

        if path == "/preview-powerbi":
            visual_html = PBI_DIR / "powerbi_visual_preview.html"
            if visual_html.exists():
                try:
                    page = visual_html.read_text(encoding="utf-8")
                    if "返回旺财控制台" not in page:
                        if "<body>" in page:
                            page = page.replace("<body>", "<body><p style='padding:12px 16px 0 16px;'><a href=\"/\">返回旺财控制台</a></p>", 1)
                        else:
                            page = f"<p><a href='/'>返回旺财控制台</a></p>{page}"
                    self._send_html(page)
                    return
                except Exception as e:
                    self._send_html(f"<h3>可视化页面读取失败：{html.escape(str(e))}</h3><p><a href='/'>返回</a></p>")
                    return

            pbi_csv = PBI_DIR / "pbi_dataset.csv"
            if pbi_csv.exists():
                try:
                    page_no = int((query.get("page", ["1"])[0] or "1").strip())
                    page_no = max(1, page_no)
                    all_df = pd.read_csv(pbi_csv)
                    per_page = 120
                    start = (page_no - 1) * per_page
                    end = start + per_page
                    df = all_df.iloc[start:end].copy()
                    preferred_cols = ["ticker", "period_key", "revenue", "net_profit", "revenue_mom_pct", "threshold_pct", "alert_flag"]
                    keep_cols = [c for c in preferred_cols if c in df.columns]
                    if keep_cols:
                        df = df[keep_cols]
                    for col in ["revenue_mom_pct", "threshold_pct"]:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors="coerce").round(4)

                    next_link = f"<a href='/preview-powerbi?page={page_no + 1}'>下一页</a>" if end < len(all_df) else ""
                    prev_link = f"<a href='/preview-powerbi?page={page_no - 1}'>上一页</a>" if page_no > 1 else ""
                    page = f"""
                    <html><head><meta charset='utf-8'><title>Power BI 数据预览</title>
                    <style>
                    body{{font-family:Microsoft YaHei,sans-serif;padding:16px;background:#fff9e8;color:#3b2a11;}}
                    .table-wrap{{width:100%;overflow-x:auto;}}
                    table{{border-collapse:collapse;width:100%;min-width:760px;background:#fff;table-layout:fixed;}}
                    th,td{{border:1px solid #e6cf9e;padding:6px 8px;font-size:13px;white-space:nowrap;text-overflow:ellipsis;overflow:hidden;}}
                    th{{background:#fff1cc;}}
                    a{{color:#1f5fbf;}}
                    </style></head><body>
                    <p><a href="/">返回旺财控制台</a></p>
                    <h2>KPI快照：{html.escape(str(pbi_csv.name))}</h2>
                    <p>第 {page_no} 页，单页 {per_page} 行。{prev_link} {next_link}</p>
                    <div class='table-wrap'>{df.to_html(index=False, border=0)}</div>
                    </body></html>
                    """
                    self._send_html(page)
                    return
                except Exception as e:
                    self._send_html(f"<h3>预览失败：{html.escape(str(e))}</h3><p><a href='/'>返回</a></p>")
                    return
            self._send_html("<h3>未找到可预览的 Power BI 数据文件。</h3><p><a href='/'>返回</a></p>")
            return

        self.send_error(HTTPStatus.NOT_FOUND, "页面不存在")

    def do_POST(self):
        global PENDING_CONFIRM

        content_length = int(self.headers.get("Content-Length", "0"))
        content_type = self.headers.get("Content-Type", "")
        body = self.rfile.read(content_length)

        if self.path == "/run":
            form = parse_qs(body.decode("utf-8", errors="ignore"))
            action = (form.get("action", [""])[0] or "").strip()
            command = ACTION_MAP.get(action)
            if not command:
                self._set_result("系统提示", "无效按钮操作，请刷新后重试。")
            else:
                try:
                    normalized_command = re.sub(r"^[0-9①②③④⑤⑥⑦⑧⑨⑩\.\s]+", "", command).strip()
                    self._set_result(command, route_command(normalized_command))
                except Exception as e:
                    self._set_result(command, f"执行失败：{e}")
            self._send_html(render_page())
            return

        if self.path == "/set-runtime-params":
            form = parse_qs(body.decode("utf-8", errors="ignore"))
            params = {
                "last_year_avg_growth": (form.get("last_year_avg_growth", [""])[0] or "").strip(),
                "expected_growth": (form.get("expected_growth", [""])[0] or "").strip(),
                "budget_amount": (form.get("budget_amount", [""])[0] or "").strip(),
                "threshold_val": (form.get("threshold_val", [""])[0] or "").strip(),
                "weight_totalRevenue": (form.get("weight_totalRevenue", [""])[0] or "").strip(),
                "weight_sellingGeneralAndAdministrative": (
                    form.get("weight_sellingGeneralAndAdministrative", [""])[0] or ""
                ).strip(),
            }
            merged = set_runtime_params(params)
            self._set_result("动态预算参数", f"参数已更新：{merged}")
            self._send_html(render_page())
            return

        if self.path == "/confirm-import":
            _, msg = confirm_staged_import()
            self._set_result("确认完成导入", msg)
            self._send_html(render_page())
            return

        if self.path == "/api/v1/confirm_approval":
            form = {}
            try:
                form = parse_qs(body.decode("utf-8", errors="ignore"))
            except Exception:
                form = {}
            result = route_command("审批通过，更新看板")
            self._set_result("API审批并触发计算", result)

            if (form.get("redirect", [""])[0] or "") == "1":
                self._redirect("/")
                return
            self._send_json({"ok": True, "message": result})
            return

        if self.path == "/cloud-ingest":
            form = parse_qs(body.decode("utf-8", errors="ignore"))
            txt = (form.get("cloud_input", [""])[0] or "").strip()
            if not txt:
                self._set_result("云端取数", "请输入股票代码、ERP账套名称或API地址。")
            else:
                _, msg = cloud_ingest(txt)
                self._set_result("云端取数", msg)
            self._send_html(render_page())
            return

        if self.path == "/local-import":
            _, files = parse_multipart(body, content_type)
            if "file" not in files:
                self._set_result("本地表单导入", "未接收到文件，请重试。")
                self._send_html(render_page())
                return

            fobj = files["file"]
            filename = fobj["filename"] or "upload.csv"
            ext = Path(filename).suffix.lower()
            if ext not in [".csv", ".xlsx", ".xls"]:
                self._set_result("本地表单导入", "仅支持 CSV / Excel 文件。")
                self._send_html(render_page())
                return

            UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
            PENDING_DIR.mkdir(parents=True, exist_ok=True)
            safe_name = f"upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}{ext}"
            saved = UPLOAD_DIR / safe_name
            saved.write_bytes(fobj["content"])

            try:
                if ext in [".xlsx", ".xls"]:
                    df = pd.read_excel(saved)
                else:
                    df = pd.read_csv(saved)
            except Exception as e:
                self._set_result("本地表单导入", f"文件读取失败：{e}")
                self._send_html(render_page())
                return

            sugg = suggest_mapping(list(df.columns))
            if not sugg.get("fiscal_date") and len(df.columns) > 0:
                sugg["fiscal_date"] = str(df.columns[0])
            if not sugg.get("amount"):
                for c in df.columns:
                    if pd.to_numeric(df[c], errors="coerce").notna().sum() > 0:
                        sugg["amount"] = str(c)
                        break

            has_core = bool(sugg.get("fiscal_date") and sugg.get("amount"))
            if has_core:
                std = standardize_with_mapping(df, sugg, default_ticker="MANUAL")
                _, msg = stage_dataframe(std if not std.empty else df, source_name=f"Upload:{saved.name}")
                self._set_result("本地表单导入", msg)
                self._send_html(render_page())
                return

            token = uuid.uuid4().hex
            pending_file = PENDING_DIR / f"pending_{token}.csv"
            df.to_csv(pending_file, index=False, encoding="utf-8-sig")
            PENDING_CONFIRM = {
                "token": token,
                "file": str(pending_file),
                "columns": [str(c) for c in df.columns],
                "suggestion": sugg,
            }
            self._set_result("本地表单导入", "已进入智能识别确认，请在下方确认字段映射后加入暂存区。")
            self._send_html(render_page())
            return

        if self.path == "/confirm-mapping":
            form = parse_qs(body.decode("utf-8", errors="ignore"))
            decision = (form.get("decision", [""])[0] or "").strip()
            token = (form.get("token", [""])[0] or "").strip()

            if not PENDING_CONFIRM or token != PENDING_CONFIRM.get("token"):
                self._set_result("智能字段确认", "未找到待确认任务，请重新上传文件。")
                self._send_html(render_page())
                return

            if decision == "cancel":
                PENDING_CONFIRM = {}
                self._set_result("智能字段确认", "已取消本次映射，请重新上传。")
                self._send_html(render_page())
                return

            src = Path(PENDING_CONFIRM["file"])
            if not src.exists():
                PENDING_CONFIRM = {}
                self._set_result("智能字段确认", "待处理文件不存在，请重新上传。")
                self._send_html(render_page())
                return

            try:
                df = pd.read_csv(src)
            except Exception as e:
                PENDING_CONFIRM = {}
                self._set_result("智能字段确认", f"读取待处理文件失败：{e}")
                self._send_html(render_page())
                return

            mapping = {
                "fiscal_date": (form.get("fiscal_date", [""])[0] or form.get("fiscal_date_suggest", [""])[0]).strip(),
                "amount": (form.get("amount", [""])[0] or form.get("amount_suggest", [""])[0]).strip(),
                "account_name": (form.get("account_name", [""])[0] or form.get("account_name_suggest", [""])[0]).strip(),
                "ticker": (form.get("ticker", [""])[0] or form.get("ticker_suggest", [""])[0]).strip(),
            }
            default_ticker = (form.get("default_ticker", ["MANUAL"])[0] or "MANUAL").strip().upper()

            std = standardize_with_mapping(df, mapping, default_ticker=default_ticker)
            if std.empty:
                self._set_result("智能字段确认", "映射后无有效数据，请检查月份列和金额列。")
                self._send_html(render_page())
                return

            _, msg = stage_dataframe(std, source_name=f"Mapped:{src.name}")
            PENDING_CONFIRM = {}
            self._set_result("智能字段确认", msg)
            self._send_html(render_page())
            return

        self.send_error(HTTPStatus.NOT_FOUND, "页面不存在")

    def log_message(self, fmt, *args):
        return


def main():
    print(f"旺财 Web 已启动：http://{HOST}:{PORT}")
    print("按 Ctrl + C 停止服务")
    server = ThreadingHTTPServer((HOST, PORT), CommanderHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
