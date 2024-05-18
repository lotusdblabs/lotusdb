# Release 2.1.0 (2024-05-19)
## 🚀 New Features
* Feature: support iterator (https://github.com/lotusdblabs/lotusdb/pull/153) @akiozihao

## 🎄 Enhancements
* fix: Pass corresponding options to db.With... methods (https://github.com/lotusdblabs/lotusdb/pull/159) @akiozihao
* feat(lint): add golangci-lint (https://github.com/lotusdblabs/lotusdb/pull/160) @akiozihao

# Release 2.0.0 (2023-09-04)

## 🚀 New Features
* Change the architecture of LotusDB
  * Use wal project to store Write-Ahead-Log and Value-Log
  * Hash key into different indexes and value log files
  * Index and Vlog read/write will be concurrent
* Basic Put/Get/Delete/Exist operations
* Batch Put/Delete operations
* Value log compaction
