# Hash-Based Delta Checker for SQL Server (Environment Comparison)

![SQL](https://img.shields.io/badge/SQL-Server-blue)
![Data Governance](https://img.shields.io/badge/Data-Governance-purple)
![Observability](https://img.shields.io/badge/Data-Observability-orange)
![Hash](https://img.shields.io/badge/Hash-SHA2_256-yellow)

This script compares the state of a table between two environments
(typically Production vs Homologation) using deterministic hashing and
set operations (EXCEPT) without creating permanent objects or requiring
special permissions.

### ✅ Why?
Silent regressions happen:
- Values slightly changed
- Rows inserted unexpectedly
- Rows missing after deploy
- Fields altered by compensation logic

Traditional comparison (column-by-column) is heavy, noisy and error-prone.

This approach is **state-based**:
if the hash changes, the row changed.

---

## 🚀 Key Features

- Detects:
  - New rows
  - Deleted rows
  - Altered rows
- No CREATE TABLE permissions required
- Runs entirely on temp objects
- Optional case sensitivity
- Invisible separator to avoid collisions
- Debug flag to print dynamic SQL
- Does not require Database Mail

---

## 🧠 Theory

This solution combines:

- Deterministic hashing (`SHA2_256`)
- ISO date normalization (format 126)
- Set theory (`EXCEPT`)
- Row-level state comparison

It’s the same principle used in warehouse reconciliation engines.

---

## 🧪 Requirements

- SQL Server 2016+
- `HASHBYTES` enabled

---

## ⚙️ Parameters

| Param               | Description |
|---------------------|-------------|
| @TabelaProducao     | Dataset A |
| @TabelaHomologacao  | Dataset B |
| @ColunasChave       | Composite key |
| @ColunasExcecao     | Columns to ignore |
| @Limitador          | Optional environment cut |
| @VerificaCase       | Enable case sensitivity |
| @Debug              | Print dynamic SQL |

---

## 📦 Output Categories

| Category | Meaning |
|----------|---------|
| Delta Novos | Rows present only in Homolog |
| Delta Excluídos | Rows present only in Prod |
| Delta Hash | Same key, different content |

---

## 📊 Visual Model

