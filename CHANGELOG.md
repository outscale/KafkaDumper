# 📜 Changelog

All notable changes to this project will be documented in this file.  
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)  
and this project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### 💥 Breaking
- Migrating from GitLab CI to GitHub Actions

### ✨ Added
- (nothing yet)

### 🛠️ Changed / Refactoring
- Optional specification on tag jobs

### 📝 Documentation
- (nothing yet)

### ⚰️ Deprecated
- (nothing yet)

### 🗑️ Removed
- (nothing yet)

### 🐛 Fixed
- (nothing yet)

### 🔒 Security
- Update lz4_flex version [RUSTSEC-2026-0041](https://rustsec.org/advisories/RUSTSEC-2026-0041.html)

### 📦 Dependency updates
- (nothing yet)

### 🌱 Others
- (nothing yet)

---

## [0.2.5] - 2026-03-04

### ✨ Added
- Add cargo nextest to the pipeline
- Change default file name (on condition)

### 🛠️ Changed / Refactoring
- Overall optimization of CI

---

## [0.2.4] - 2026-03-03

### ✨ Added
- Automatic modification of the homebrew formula

### 📝 Documentation
- Update README.md with installation instruction

---

## [0.2.3] - 2026-03-03

### 🐛 Fixed
- Fix automatic release

---

## [0.2.2] - 2026-03-02

### ✨ Added
- Export/Import to json according to the schemaregistry version
- Implementation of automatic ci/cd release

---

## [0.2.1] - 2026-02-24

### ✨ Added
- Option to specify an offset range as input

### 🐛 Fixed
- Added an error message earlier when ``count`` is 0 (prevents the infinite loading bug)

---

## [0.2.0] - 2026-02-17

### ✨ Added
- Option to import each message into its original thread
- Provide multiple input fields during import
- Implementation of automated builds for various platforms

---

## [0.1.0] - 2026-01-28

### ✨ Added
- Initial release of the project.
- A minimalist system for dumping Kafka data into Parquet format

---

## 🔑 How to use this file

1. During development:
   - Add entries under **[Unreleased]** in the right category.
   - Keep the wording concise and clear.
   - Link related issues/PRs where relevant (`#123`, [PR-456](https://github.com/org/repo/pull/456)).

2. When preparing a release:
   - Move items from **[Unreleased]** into a new version section (`[X.Y.Z] - YYYY-MM-DD`).
   - Leave **[Unreleased]** empty for future changes.

3. At release time:
   - GitHub automatically generates release notes from PR labels (`.github/release.yml`).
   - This `CHANGELOG.md` is the **permanent record** in the repository.