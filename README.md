# Cross-Platform Backup Tool

A flexible and powerful backup solution for Windows and Linux systems. Supports full and differential backups, compression, and customizable scheduling.

[English](#english) | [日本語](#japanese)

<a id="english"></a>
## English

### Features
- Cross-platform compatibility (Windows/Linux)
- File and directory backup with symlink support
- Full and differential backup options
- ZIP and TAR.GZ compression
- Flexible scheduling (daily, weekly, monthly)
- Command-line interface for easy automation
- Network and external drive support

### Prerequisites
- Python 3.6 or later
- No external dependencies required (uses standard library only)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/backup-tool.git
   cd backup-tool
   ```

2. Make the script executable (Linux):
   ```bash
   chmod +x backup.py
   ```

### Basic Usage

1. Add backup sources:
   ```bash
   python backup.py add-source /path/to/source
   ```

2. Set backup destination:
   ```bash
   python backup.py set-destination /path/to/backup/destination
   ```

3. Configure backup settings:
   ```bash
   # Set backup type (full or differential)
   python backup.py set-type differential

   # Enable compression
   python backup.py set-compress --enable --format zip

   # Set schedule (daily, weekly, monthly)
   python backup.py set-schedule weekly --time 00:00 --day 0 --full-day 0
   ```

4. Run backup:
   ```bash
   python backup.py run
   ```

5. List backup history:
   ```bash
   python backup.py list
   ```

6. View current configuration:
   ```bash
   python backup.py show-config
   ```

### Scheduling

#### Windows

1. Open Task Scheduler
2. Create a new Basic Task
3. Set trigger (daily, weekly, etc.)
4. Action: Start a program
5. Program: `python` or `pythonw`
6. Arguments: `C:\path\to\backup.py run`

#### Linux

Add to crontab:
```bash
crontab -e
# Add line (example: run daily at midnight):
0 0 * * * /usr/bin/python3 /path/to/backup.py run
```

### Troubleshooting

- Check log file (`backup.log`) for error messages
- Verify destination directory exists and is writable
- Ensure source paths are correct
- If configuration is corrupted, remove `~/.backup_config.json` and reconfigure

### License
MIT License

---

<a id="japanese"></a>
## 日本語

### 機能
- クロスプラットフォーム対応（Windows/Linux）
- シンボリックリンク対応のファイル・ディレクトリバックアップ
- 完全バックアップと差分バックアップのオプション
- ZIPおよびTAR.GZ圧縮形式対応
- 柔軟なスケジュール設定（日次、週次、月次）
- 自動化しやすいコマンドラインインターフェース
- ネットワークドライブと外部ドライブのサポート

### 前提条件
- Python 3.6以上
- 外部依存ライブラリなし（標準ライブラリのみ使用）

### インストール

1. このリポジトリをクローン：
   ```bash
   git clone https://github.com/yourusername/backup-tool.git
   cd backup-tool
   ```

2. スクリプトに実行権限を付与（Linuxの場合）：
   ```bash
   chmod +x backup.py
   ```

### 基本的な使い方

1. バックアップ元を追加：
   ```bash
   python backup.py add-source /path/to/source
   ```

2. バックアップ先を設定：
   ```bash
   python backup.py set-destination /path/to/backup/destination
   ```

3. バックアップ設定を構成：
   ```bash
   # バックアップタイプを設定（完全または差分）
   python backup.py set-type differential

   # 圧縮を有効化
   python backup.py set-compress --enable --format zip

   # スケジュール設定（毎日、毎週、毎月）
   python backup.py set-schedule weekly --time 00:00 --day 0 --full-day 0
   ```

4. バックアップを実行：
   ```bash
   python backup.py run
   ```

5. バックアップ履歴を表示：
   ```bash
   python backup.py list
   ```

6. 現在の設定を表示：
   ```bash
   python backup.py show-config
   ```

### スケジュール設定

#### Windows

1. タスクスケジューラを開く
2. 基本タスクの作成
3. トリガーを設定（毎日、毎週など）
4. 操作：プログラムの開始
5. プログラム：`python`または`pythonw`
6. 引数：`C:\path\to\backup.py run`

#### Linux

cronに追加：
```bash
crontab -e
# 以下の行を追加（例：毎日深夜0時に実行）：
0 0 * * * /usr/bin/python3 /path/to/backup.py run
```

### トラブルシューティング

- ログファイル（`backup.log`）でエラーメッセージを確認
- バックアップ先ディレクトリが存在し、書き込み権限があるか確認
- バックアップ元のパスが正しいか確認
- 設定が破損している場合は、`~/.backup_config.json`を削除して再設定

### ライセンス
MITライセンス# SecureCopy
