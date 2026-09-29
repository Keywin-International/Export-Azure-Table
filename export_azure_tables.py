"""
Azure Table Storage -> CSV 匯出工具

用來取代 260907_export azure.txt（PowerShell 版本）。
原版有 4 個會導致資料筆數偏少的問題：
  1. 第 187 行 `string.Format(...)` 是非法的 PowerShell 語法，會在第一張表跑完後讓整個腳本當掉，
     導致後面的表完全沒被匯出。
  2. 手刻的字串切割式 JSON 解析，欄位值裡只要有逗號/冒號/大括號就會把資料列切壞。
  3. 分頁時遇到任何例外就直接放棄該表剩餘的資料，沒有重試機制。
  4. CSV 欄位只採用第一筆資料的 schema，後面資料列的新欄位會被捨棄。

這支程式改用官方 azure-data-tables SDK 處理 JSON 反序列化、分頁與重試，每張表用兩輪掃描
（第一輪只收集欄位名稱、不留資料列，第二輪邊讀邊寫不整表留在記憶體）確保欄位不遺漏，
同時用多執行緒同時匯出多張表以加快速度。
"""

import csv
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from azure.core.credentials import AzureNamedKeyCredential
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.data.tables import TableServiceClient

# ================= 1. 請在此修改您的設定 =================
STORAGE_ACCOUNT_NAME = "fleetivityevents"
# 建議把金鑰放在環境變數 AZURE_STORAGE_KEY 中；找不到環境變數時才使用下面這組預設值當備援。
STORAGE_ACCOUNT_KEY = os.environ.get(
    "AZURE_STORAGE_KEY",
    "rbteMKl7S/kJrxEHZZ+/96xD5GK4RlTsPSfGW1b5hI/gVz5Wq40aqiEszZe2Br2Cisz6AXIXpfA2mHBb8flI7g==",
)
LOCAL_EXPORT_PATH = r"\\192.168.0.214\研發三部\Product\SWIFT\data\Azure\Table\0921"
MAX_WORKERS = 6
RETRY_TOTAL = 6
RETRY_BACKOFF_FACTOR = 1.0
# 只匯出這個年份的資料（依各表最合適的業務日期欄位判斷，見下方 TABLE_YEAR_FILTER）。
FILTER_YEAR: Optional[int] = 2026
# =========================================================

# 2. 您指定要匯出的關鍵核心資料表清單（已去除原始清單中的重複項目，共 23 張）
#    其中 ProductVolumeTotal / WasteVolumeRawDataTemp / ProductVolumeRawDataTemp
#    經內部文件 AzureTable清單.csv 確認「Azure 目前沒有這張表」，保留在清單中，
#    若查不到會標記為「表不存在(略過)」而非失敗。
TARGET_TABLES = [
    "SWIFTTrackSheet",
    "PlateRawData",
    "WasteVolumeRawData",
    "WasteVolumeManual",
    "WasteVolumeTotal",
    "ProductRawData",
    "ProductVolumeRawData",
    "ProductVolumeManual",
    "ProductVolumeTotal",
    "DailyInputLog",
    "DailyShipmentLog",
    "DailyVolumeStatistics",
    "MonthlyInputLog",
    "MonthlyShipmentLog",
    "MonthlyStockAnalyze",
    "CheckPointStatusTable",
    "DailyAlertStatistics",
    "SWIFTAlertTable",
    "WasteVolumeCheckPointfortest",
    "ProductVolumeCheckPointfortest",
    "WasteVolumeRawDataTemp",
    "ProductVolumeRawDataTemp",
    "FurnaceTempRawData",
]

BASE_COLUMNS = ["PartitionKey", "RowKey", "Timestamp"]

# 3. 每張表用來判斷「年份」的業務欄位/方式（實測 Azure 即時資料才決定，見下方說明）。
#    ("server", 欄位, lo, hi)     -> 該欄位格式已驗證為零填充、年份在前，字串排序等同日期排序，
#                                    用 Azure OData 查詢在伺服端篩選（省頻寬、省時間）。
#    ("server_pk"/"server_rk", None, lo, hi) -> PartitionKey/RowKey 本身就是乾淨的 YYYYMM 或
#                                    YYYYMMDD 字串，同樣可在伺服端篩選。
#    ("client", 欄位, None, None) -> 實測發現該欄位同一張表內混用多種字串格式，甚至混用
#                                    字串／Azure 原生日期型別（例如 ProductRawData 的 RTC：
#                                    2026 年起的資料已改用原生日期型別，之前是
#                                    "MM/DD/YYYY HH:MM:SS +00:00" 這種年份不在前面、無法用
#                                    字串範圍正確篩選的格式）。這類表改成整表掃描，逐筆用
#                                    extract_year() 解析後再決定是否寫入，正確性優先於速度。
TABLE_YEAR_FILTER = {
    "CheckPointStatusTable": ("server", "Date", "{y}/01/01", "{y1}/01/01"),
    "ProductVolumeManual": ("server", "DataDate", "{y}-01-01", "{y1}-01-01"),
    "WasteVolumeManual": ("server", "DataDate", "{y}-01-01", "{y1}-01-01"),
    "MonthlyInputLog": ("server_pk", None, "{y}01", "{y1}01"),
    "MonthlyShipmentLog": ("server_pk", None, "{y}01", "{y1}01"),
    "MonthlyStockAnalyze": ("server_pk", None, "{y}01", "{y1}01"),
    "DailyAlertStatistics": ("server_pk", None, "{y}0101", "{y1}0101"),
    "DailyInputLog": ("server_pk", None, "{y}0101", "{y1}0101"),
    "DailyShipmentLog": ("server_pk", None, "{y}0101", "{y1}0101"),
    "DailyVolumeStatistics": ("server_pk", None, "{y}0101", "{y1}0101"),
    "ProductVolumeCheckPointfortest": ("server_rk", None, "{y}0101", "{y1}0101"),
    "WasteVolumeCheckPointfortest": ("server_rk", None, "{y}0101", "{y1}0101"),
    "SWIFTTrackSheet": ("client", "Rec_Date", None, None),
    "FurnaceTempRawData": ("client", "RTC", None, None),
    "PlateRawData": ("client", "RTC", None, None),
    "ProductRawData": ("client", "RTC", None, None),
    "ProductVolumeRawData": ("client", "RTC", None, None),
    "WasteVolumeRawData": ("client", "RTC", None, None),
    "WasteVolumeTotal": ("client", "RTC", None, None),
    "SWIFTAlertTable": ("client", "LastTimeAlertDate", None, None),
    # 其餘表（ProductVolumeTotal / WasteVolumeRawDataTemp / ProductVolumeRawDataTemp）
    # 目前 Azure 上沒有資料，不受篩選影響，未列入不影響行為。
}

_YEAR_TOKEN_RE = re.compile(r"(?<!\d)(\d{4})(?!\d)")


def extract_year(value) -> Optional[int]:
    # 同一個業務日期欄位在同一張表內，可能同時出現字串（好幾種不同格式，包含中文上午/下午
    # 時間標記）跟 Azure 原生日期型別（TablesEntityDatetime 是 datetime 的子類別）。
    # 不管哪一種格式，我們只需要「年份」，所以原生日期型別直接讀 .year；字串則用「找一個
    # 前後都不是數字的連續 4 位數字」這個寬鬆但可靠的規則抓年份 —— 已用即時 Azure 資料
    # 驗證過，這個規則在所有實際出現過的格式下都能正確抓到年份，不會誤抓月/日/時分秒
    # （因為那些都是 1~2 位數字，只有年份是連續 4 位數字）。
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.year
    if isinstance(value, str):
        m = _YEAR_TOKEN_RE.search(value)
        if m:
            return int(m.group(1))
    return None


def build_server_query_filter(table_name: str) -> tuple[Optional[str], Optional[dict]]:
    if FILTER_YEAR is None:
        return None, None
    spec = TABLE_YEAR_FILTER.get(table_name)
    if not spec:
        return None, None
    kind, field, lo_tmpl, hi_tmpl = spec
    if kind == "client":
        return None, None
    lo = lo_tmpl.format(y=FILTER_YEAR, y1=FILTER_YEAR + 1)
    hi = hi_tmpl.format(y=FILTER_YEAR, y1=FILTER_YEAR + 1)
    key_field = {"server": field, "server_pk": "PartitionKey", "server_rk": "RowKey"}[kind]
    return f"{key_field} ge @lo and {key_field} lt @hi", {"lo": lo, "hi": hi}


def get_client_filter_field(table_name: str) -> Optional[str]:
    if FILTER_YEAR is None:
        return None
    spec = TABLE_YEAR_FILTER.get(table_name)
    if spec and spec[0] == "client":
        return spec[1]
    return None


logger = logging.getLogger("export_azure_tables")


@dataclass
class TableExportResult:
    table_name: str
    status: str  # "success" | "empty" | "not_found" | "failed"
    row_count: int
    elapsed_seconds: float
    error: Optional[str] = None


def setup_logging(export_dir: Path) -> Path:
    # 主控台編碼不一定是 UTF-8（例如舊版 cmd.exe 用 Big5/CP950），中文/emoji 訊息可能會
    # 讓 StreamHandler 寫入失敗並印出干擾性的 "Logging error"。這裡強制主控台輸出用 UTF-8，
    # 遇到無法顯示的字元就替換掉，而不是讓 log 整條寫入失敗。
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")

    # log 檔案故意寫在本機（腳本旁邊的 logs 資料夾），不寫到網路硬碟：曾實測長時間
    # （2 小時以上）大量讀寫網路分享後，SMB 連線會暫時不穩，導致寫在網路硬碟上的 log
    # 檔案 handle 失效（OSError: Invalid argument），進而讓整個匯出程序中斷。CSV 資料
    # 本身仍然輸出到網路硬碟，只有 log 改成本機以避免這個問題拖垮整批匯出。
    local_log_dir = Path(__file__).resolve().parent / "logs"
    local_log_dir.mkdir(parents=True, exist_ok=True)
    log_path = local_log_dir / f"_export_log_{datetime.now():%Y%m%d_%H%M%S}.txt"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
    )
    # azure-core 預設會在 INFO 等級把每次 HTTP 請求/回應的完整標頭都印出來，
    # 資料量一大 log 檔會暴增到數十 MB，這裡只留 WARNING 以上（例如真正的重試/錯誤）。
    logging.getLogger("azure").setLevel(logging.WARNING)
    return log_path


def build_table_service_client() -> TableServiceClient:
    credential = AzureNamedKeyCredential(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    endpoint = f"https://{STORAGE_ACCOUNT_NAME}.table.core.windows.net"
    return TableServiceClient(
        endpoint=endpoint,
        credential=credential,
        retry_total=RETRY_TOTAL,
        retry_backoff_factor=RETRY_BACKOFF_FACTOR,
    )


def entity_to_row(entity) -> dict:
    # Timestamp 是 Azure 系統管理的欄位，azure-data-tables 不會把它放進 entity 本身的
    # dict 內容裡（entity.items() 拿不到），必須另外從 entity.metadata['timestamp'] 取得。
    row = {k: ("" if v is None else str(v)) for k, v in entity.items()}
    ts = entity.metadata.get("timestamp")
    if ts is not None:
        row["Timestamp"] = str(ts)
    return row


def _iter_entities(table_client, query_filter: Optional[str], params: Optional[dict], select=None):
    if query_filter is not None:
        return table_client.query_entities(query_filter=query_filter, parameters=params, select=select)
    return table_client.list_entities(select=select)


def export_one_table(
    service_client: TableServiceClient, table_name: str, export_dir: Path
) -> TableExportResult:
    start = time.monotonic()
    table_client = service_client.get_table_client(table_name)
    csv_path = export_dir / f"{table_name}.csv"

    query_filter, params = build_server_query_filter(table_name)
    client_field = get_client_filter_field(table_name)
    skipped_unparseable = 0

    try:
        # Pass 1：完整掃過整張表一次，只累積「出現過的欄位名稱」（依首次出現順序），
        # 不保留任何資料列內容 —— 記憶體成本只跟「欄位數」有關，跟筆數無關。
        # 這修正了先前「只取樣前 N 筆決定欄位」的做法：曾實測發現某些欄位
        # （例如 SWIFTTrackSheet 的 Tre_Date／TreMetNo）雖然佔了 15~18% 的資料列，
        # 但因為依 PartitionKey 排序後集中出現在較後面的公司/區段，取樣前 2000 筆
        # 完全掃不到，導致該欄位整欄被靜默漏掉。改成完整掃描欄位名稱，正確性優先。
        # 若有指定 FILTER_YEAR：能在伺服端篩選的表（query_filter 有值）只會掃到目標年份的
        # 資料；其餘表（client_field 有值）仍整表掃描，靠 extract_year() 逐筆判斷是否計入。
        columns = list(BASE_COLUMNS)
        seen = set(columns)
        row_count = 0
        for entity in _iter_entities(table_client, query_filter, params):
            if client_field is not None and extract_year(entity.get(client_field)) != FILTER_YEAR:
                continue
            row_count += 1
            for key in entity.keys():
                if key not in seen:
                    seen.add(key)
                    columns.append(key)

        if row_count == 0:
            if csv_path.exists():
                csv_path.unlink()
            elapsed = time.monotonic() - start
            logger.info("⚪ [%s] 無任何資料，已略過。", table_name)
            return TableExportResult(table_name, "empty", 0, elapsed)

        # Pass 2：再掃一次，這次邊讀邊串流寫檔，不整表留在記憶體。
        written = 0
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore", restval="")
            writer.writeheader()
            for entity in _iter_entities(table_client, query_filter, params):
                if client_field is not None:
                    year = extract_year(entity.get(client_field))
                    if year != FILTER_YEAR:
                        if year is None:
                            skipped_unparseable += 1
                        continue
                writer.writerow(entity_to_row(entity))
                written += 1
        row_count = written

        elapsed = time.monotonic() - start
        if skipped_unparseable:
            logger.warning(
                "⚠ [%s] 有 %d 筆資料因 %s 欄位無法判斷年份，已略過（不計入 %s 年匯出結果）",
                table_name, skipped_unparseable, client_field, FILTER_YEAR,
            )
        year_note = f"（僅 {FILTER_YEAR} 年）" if FILTER_YEAR is not None else ""
        logger.info("✅ [%s] 成功匯出 %d 筆%s，耗時 %.1f 秒", table_name, row_count, year_note, elapsed)
        return TableExportResult(table_name, "success", row_count, elapsed)

    except ResourceNotFoundError:
        elapsed = time.monotonic() - start
        logger.info("⚪ [%s] 表不存在，已略過。", table_name)
        return TableExportResult(table_name, "not_found", 0, elapsed)

    except (HttpResponseError, ServiceRequestError, OSError) as e:
        elapsed = time.monotonic() - start
        logger.error("❌ [%s] 匯出失敗：%s", table_name, e)
        return TableExportResult(table_name, "failed", 0, elapsed, error=str(e))


def run_all_exports(
    service_client: TableServiceClient, table_names: list[str], export_dir: Path
) -> list[TableExportResult]:
    results: list[TableExportResult] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(export_one_table, service_client, name, export_dir): name
            for name in table_names
        }
        for i, future in enumerate(as_completed(futures), start=1):
            name = futures[future]
            try:
                result = future.result()
            except Exception as e:  # 保底：任何未預期例外都不能讓其他表跟著中斷
                logger.exception("❌ [%s] 發生未預期例外", name)
                result = TableExportResult(name, "failed", 0, 0.0, error=str(e))
            results.append(result)
            logger.info("[%d/%d] %s 完成 (狀態: %s)", i, len(table_names), name, result.status)
    return results


def print_summary(results: list[TableExportResult]) -> None:
    status_order = {"failed": 0, "success": 1, "not_found": 2, "empty": 3}
    results_sorted = sorted(results, key=lambda r: status_order.get(r.status, 9))

    status_label = {
        "success": "成功",
        "empty": "無資料略過",
        "not_found": "表不存在(略過)",
        "failed": "失敗",
    }

    print("\n" + "=" * 70)
    print("                 任務完成！資料匯出總結報告")
    print("=" * 70)
    header = f"{'資料表名稱':<32}{'匯出筆數':>10}{'花費時間':>12}{'狀態':>14}"
    print(header)
    print("-" * 70)
    for r in results_sorted:
        elapsed_str = f"{int(r.elapsed_seconds // 60):02d}分{int(r.elapsed_seconds % 60):02d}秒"
        status_str = status_label.get(r.status, r.status)
        if r.status == "failed" and r.error:
            status_str = f"失敗:{r.error[:40]}"
        print(f"{r.table_name:<32}{r.row_count:>10}{elapsed_str:>12}{status_str:>14}")
    print("=" * 70)

    failed_count = sum(1 for r in results if r.status == "failed")
    print(f"總計 {len(results)} 張表，失敗 {failed_count} 張。")


def main() -> int:
    export_dir = Path(LOCAL_EXPORT_PATH)
    export_dir.mkdir(parents=True, exist_ok=True)
    log_path = setup_logging(export_dir)
    logger.info("開始批次匯出 %d 張資料表，Log 檔：%s", len(TARGET_TABLES), log_path)

    client = build_table_service_client()
    results = run_all_exports(client, TARGET_TABLES, export_dir)
    print_summary(results)

    return 1 if any(r.status == "failed" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
