# Export Azure — Azure Table Storage 匯出工具

用 Python 重寫 `..\260907_export azure.txt`（PowerShell 版），改用官方 `azure-data-tables`
SDK，修正原本會導致「匯出筆數偏少」的問題。

## 為什麼要重寫

原本的 PowerShell 腳本有 4 個會造成資料遺失/不完整的問題：

1. **腳本會在第一張表跑完後整個當掉**：第 187 行 `string.Format(...)` 是 C# 語法，不是合法
   的 PowerShell 語法，會丟出「命令不存在」的終止性錯誤，導致清單裡第一張表以後的所有表
   完全沒被處理到。**這是筆數大幅偏少的主因。**
2. **手刻的字串切割式 JSON 解析**：只要欄位值裡有逗號、冒號、大括號、中括號（例如備註/
   警報文字欄位），資料列就會被切壞、遺失或合併。
3. **分頁失敗沒有重試機制**：遇到逾時或 Azure 限流就直接放棄該表剩餘資料，且仍顯示「成功」。
4. **CSV 欄位只採用第一筆資料的 schema**，後面資料列的新欄位會被靜默捨棄。

新版改用 `azure-data-tables` SDK 處理 JSON 反序列化、分頁與（透過 `azure-core`）自動重試；
每張表對 Azure 掃兩遍 — 第一遍只收集出現過的欄位名稱（不留任何資料列內容，記憶體成本
與筆數無關），決定 CSV 表頭後，第二遍邊讀邊寫、不整表留在記憶體，同時用多執行緒同時
匯出多張表以加快速度。

> 補充（修正紀錄）：中間曾經試過「只取樣前 2000 筆算欄位」以節省一次網路掃描，但實際
> 驗證發現 `SWIFTTrackSheet` 的 `Tre_Date`／`TreMetNo` 兩個欄位雖然佔了 15~18% 的資料列，
> 卻因為依 PartitionKey 排序後集中在較後面的公司區段，取樣前 2000 筆完全掃不到，導致
> 整欄資料被靜默漏掉——這正是原本要避免的問題重新以另一種形式出現。已改回完整兩遍
> 掃描，正確性優先於節省的那一次網路掃描時間。

## 安裝

```powershell
pip install -r requirements.txt
```

## 設定

打開 `export_azure_tables.py` 最上方的設定區塊：

| 變數 | 說明 |
|---|---|
| `STORAGE_ACCOUNT_NAME` | Azure Storage 帳號名稱 |
| `STORAGE_ACCOUNT_KEY` | 存取金鑰。優先讀取環境變數 `AZURE_STORAGE_KEY`，若沒有設定環境變數才使用檔案裡的預設值 |
| `LOCAL_EXPORT_PATH` | CSV 輸出資料夾（可為本機路徑或 UNC 網路路徑），不存在時會自動建立 |
| `TARGET_TABLES` | 要匯出的資料表清單 |
| `MAX_WORKERS` | 同時併發匯出的表數量，預設 6 |
| `RETRY_TOTAL` / `RETRY_BACKOFF_FACTOR` | 單一請求遇到逾時/限流時的重試次數與退避係數 |

若要用環境變數設定金鑰（建議），在執行前先設定：

```powershell
$env:AZURE_STORAGE_KEY = "你的金鑰"
python export_azure_tables.py
```

## 執行

```powershell
python export_azure_tables.py
```

執行完會在 `LOCAL_EXPORT_PATH` 底下產生：

- 每張表一個 `<TableName>.csv`（UTF-8 with BOM，方便 Excel 開啟不亂碼；欄位順序為
  `PartitionKey, RowKey, Timestamp` 開頭，再接其餘依出現順序排列的欄位）
- 一份 `_export_log_<時間戳>.txt`，記錄整次執行的完整過程（原 PowerShell 版本只印在
  主控台，關掉視窗就沒了）

程式結束時的總結報告會列出每張表的筆數、耗時與狀態：

| 狀態 | 意義 |
|---|---|
| 成功 | 正常匯出，`<TableName>.csv` 已產生 |
| 無資料略過 | 表存在但目前沒有任何資料列 |
| 表不存在(略過) | Azure 上查無此表（例如 `ProductVolumeTotal`、`WasteVolumeRawDataTemp`、`ProductVolumeRawDataTemp` 這 3 張已知目前不存在的表），視為正常情況，不算失敗 |
| 失敗 | 真正的錯誤（例如認證失敗、逾時重試後仍失敗），會顯示錯誤訊息片段 |

若有任何表狀態為「失敗」，程式的 exit code 會是 1，方便排程/腳本判斷是否需要重跑或通知。

## 已知會回報「表不存在(略過)」的表

依內部文件 `..\AzureTable清單.csv` 記載，以下 3 張表目前在 Azure 上不存在（寫入條件目前
無公司符合，或僅供 demo 公司使用），保留在清單中是為了未來一旦開始有資料時能自動被抓到：

- `ProductVolumeTotal`
- `WasteVolumeRawDataTemp`
- `ProductVolumeRawDataTemp`
