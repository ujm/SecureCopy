# Cross-Platform Backup Tool

A flexible and powerful backup solution for Windows and Linux systems. Supports full and differential backups, compression, customizable scheduling, and remote backup via SFTP/SSH.

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
- **SFTP/SSH remote backup destination**
- Local network and external drive support
- Backup restoration (local and remote)

### Prerequisites
- Python 3.6 or later
- [paramiko](https://www.paramiko.org/) (required for SFTP/SSH backup only)

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/ujm/SecureCopy.git
   cd SecureCopy
   ```

2. Make the script executable (Linux):
   ```bash
   chmod +x SyncVault.py
   ```

3. Install dependencies for SFTP support (optional):
   ```bash
   pip install paramiko
   ```

### Basic Usage

1. Add backup sources:
   ```bash
   python SyncVault.py add-source /path/to/source
   ```

2. Set backup destination:
   ```bash
   # Local destination
   python SyncVault.py set-destination /path/to/backup/destination

   # SFTP/SSH destination
   python SyncVault.py set-sftp-destination \
     --host backup.example.com \
     --user myuser \
     --key-file ~/.ssh/id_rsa \
     --remote-path /backups \
     --port 22 \
     --max-generations 5
   ```

3. Configure backup settings:
   ```bash
   # Set backup type (full or differential)
   python SyncVault.py set-type differential

   # Enable compression
   python SyncVault.py set-compress --enable --format zip

   # Set schedule (daily, weekly, monthly)
   python SyncVault.py set-schedule weekly --time 00:00 --day 0 --full-day 0
   ```

4. Run backup:
   ```bash
   python SyncVault.py run
   ```

5. List backup history:
   ```bash
   python SyncVault.py list
   ```

6. View current configuration:
   ```bash
   python SyncVault.py show-config
   ```

7. Restore a backup:
   ```bash
   # Restore latest backup
   python SyncVault.py restore --latest /path/to/restore/destination

   # Restore by catalog ID
   python SyncVault.py restore --id 3 /path/to/restore/destination
   ```

### SFTP/SSH Backup

SFTP backup uses SSH key authentication only (no password). It works with any standard SFTP server (OpenSSH, etc.).

#### Setup

```bash
# Configure SFTP destination
python SyncVault.py set-sftp-destination \
  --host backup.example.com \
  --user backupuser \
  --key-file ~/.ssh/id_rsa \
  --remote-path /backups/myhost \
  --max-generations 7   # keep last 7 backups (0 = unlimited)

# Test the connection before running
python SyncVault.py test-connection

# Run backup
python SyncVault.py run
```

#### Host Key Verification

On the **first connection**, the server's host key is automatically saved to `~/.syncvault_known_hosts`. On subsequent connections the saved key is verified — a mismatch aborts the backup and warns of a possible MITM attack.

To reset (e.g., after a legitimate server key change):
```bash
# Remove the entry for the specific host
nano ~/.syncvault_known_hosts
```

#### Generation Management

Set `--max-generations N` to automatically delete the oldest backups when more than N exist on the server. Deletions are reflected in the local catalog and history.

#### Differential Backup with SFTP

When using differential backup, the manifest from the previous backup is downloaded from the SFTP server each time to detect changed files.

#### Scheduling with SFTP

For automated (unattended) runs, use a passphrase-free SSH key, or add the key to `ssh-agent` before the scheduled job runs.

### Scheduling

#### Windows

1. Open Task Scheduler
2. Create a new Basic Task
3. Set trigger (daily, weekly, etc.)
4. Action: Start a program
5. Program: `python` or `pythonw`
6. Arguments: `C:\path\to\SyncVault.py run`

#### Linux

Add to crontab:
```bash
crontab -e
# Add line (example: run daily at midnight):
0 0 * * * /usr/bin/python3 /path/to/SyncVault.py run
```

### Troubleshooting

- Check log file (`backup.log`) for error messages
- Verify destination directory exists and is writable
- For SFTP issues, run `python SyncVault.py test-connection` first
- If the host key error occurs after a legitimate server change, delete the entry from `~/.syncvault_known_hosts`
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
- **SFTP/SSH によるリモートバックアップ**
- ネットワークドライブと外部ドライブのサポート
- バックアップデータの復元機能（ローカル・リモート）

### 前提条件
- Python 3.6以上
- [paramiko](https://www.paramiko.org/)（SFTP/SSHバックアップを使用する場合のみ必要）

### インストール

1. このリポジトリをクローン：
   ```bash
   git clone https://github.com/ujm/SecureCopy.git
   cd SecureCopy
   ```

2. スクリプトに実行権限を付与（Linuxの場合）：
   ```bash
   chmod +x SyncVault.py
   ```

3. SFTP対応用の依存ライブラリをインストール（任意）：
   ```bash
   pip install paramiko
   ```

### 基本的な使い方

1. バックアップ元を追加：
   ```bash
   python SyncVault.py add-source /path/to/source
   ```

2. バックアップ先を設定：
   ```bash
   # ローカル保存先
   python SyncVault.py set-destination /path/to/backup/destination

   # SFTP/SSH 保存先
   python SyncVault.py set-sftp-destination \
     --host backup.example.com \
     --user myuser \
     --key-file ~/.ssh/id_rsa \
     --remote-path /backups \
     --port 22 \
     --max-generations 5
   ```

3. バックアップ設定を構成：
   ```bash
   # バックアップタイプを設定（完全または差分）
   python SyncVault.py set-type differential

   # 圧縮を有効化
   python SyncVault.py set-compress --enable --format zip

   # スケジュール設定（毎日、毎週、毎月）
   python SyncVault.py set-schedule weekly --time 00:00 --day 0 --full-day 0
   ```

4. バックアップを実行：
   ```bash
   python SyncVault.py run
   ```

5. バックアップ履歴を表示：
   ```bash
   python SyncVault.py list
   ```

6. 現在の設定を表示：
   ```bash
   python SyncVault.py show-config
   ```

7. バックアップを復元：
   ```bash
   # 最新のバックアップを復元
   python SyncVault.py restore --latest /path/to/restore/destination

   # カタログ ID を指定して復元
   python SyncVault.py restore --id 3 /path/to/restore/destination
   ```

### SFTP/SSH バックアップ

SFTP バックアップは SSH 鍵認証のみ対応しています（パスワード認証不可）。OpenSSH など標準的な SFTP サーバーで動作します。

#### セットアップ

```bash
# SFTP 保存先を設定
python SyncVault.py set-sftp-destination \
  --host backup.example.com \
  --user backupuser \
  --key-file ~/.ssh/id_rsa \
  --remote-path /backups/myhost \
  --max-generations 7   # 最新 7 世代を保持（0 = 無制限）

# 実行前に接続テスト
python SyncVault.py test-connection

# バックアップ実行
python SyncVault.py run
```

#### ホスト鍵の検証

**初回接続時**にサーバーのホスト鍵を自動的に `~/.syncvault_known_hosts` に登録します。2回目以降は保存済みの鍵と照合し、不一致の場合はバックアップを中止して警告を表示します（中間者攻撃への対策）。

正当なサーバー変更後にリセットする場合：
```bash
# 該当ホストのエントリを削除
nano ~/.syncvault_known_hosts
```

#### 世代管理

`--max-generations N` を指定すると、リモートサーバー上のバックアップが N 件を超えた際に古いものから自動削除します。削除はローカルのカタログと履歴にも反映されます。

#### SFTP での差分バックアップ

差分バックアップを使用する場合、前回のバックアップのマニフェストを毎回 SFTP サーバーからダウンロードして変更ファイルを検出します。

#### スケジュール実行（無人運転）での認証

自動実行（cron / タスクスケジューラ）では毎回パスワードを入力できないため、パスフレーズなしの SSH 鍵、または `ssh-agent` にキーを登録した状態で実行してください。

### スケジュール設定

#### Windows

1. タスクスケジューラを開く
2. 基本タスクの作成
3. トリガーを設定（毎日、毎週など）
4. 操作：プログラムの開始
5. プログラム：`python`または`pythonw`
6. 引数：`C:\path\to\SyncVault.py run`

#### Linux

cronに追加：
```bash
crontab -e
# 以下の行を追加（例：毎日深夜0時に実行）：
0 0 * * * /usr/bin/python3 /path/to/SyncVault.py run
```

### トラブルシューティング

- ログファイル（`backup.log`）でエラーメッセージを確認
- バックアップ先ディレクトリが存在し、書き込み権限があるか確認
- SFTP の問題は `python SyncVault.py test-connection` で切り分け
- 正当なサーバー変更後にホスト鍵エラーが出る場合は `~/.syncvault_known_hosts` の該当エントリを削除
- 設定が破損している場合は、`~/.backup_config.json`を削除して再設定

### ライセンス
MITライセンス
