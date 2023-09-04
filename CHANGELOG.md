# Release 2.0.0 (2023-09-04)

## 🚀 New Features
* Change the architecture of LotusDB
  * Use wal project to store Write-Ahead-Log and Value-Log
  * Hash key into different indexes and value log files
  * Index and Vlog read/write will be concurrent
* Basic Put/Get/Delete/Exist operations
* Batch Put/Delete operations
* Value log compaction
