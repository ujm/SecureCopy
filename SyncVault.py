#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
クロスプラットフォームバックアッププログラム（並列処理対応版）
Windows および Linux 環境で動作するバックアッププログラム
"""

import os
import sys
import shutil
import argparse
import logging
import datetime
import json
import tarfile
import zipfile
import fnmatch
import hashlib
import platform
import time
import posixpath
import sqlite3
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from queue import Queue
import threading

# プログレスバー表示用（オプション）
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# -----------------------------------------------------------------------
# プラットフォーム検出
# -----------------------------------------------------------------------
_SYSTEM = platform.system()   # "Windows" / "Linux" / "Darwin" など
IS_WINDOWS = _SYSTEM == "Windows"
IS_LINUX   = _SYSTEM == "Linux"


def _get_app_data_dir() -> str:
    """OS ごとのアプリケーションデータ格納ディレクトリを返す。

    * Windows: %APPDATA%\\SyncVault
    * Linux / その他: $HOME
    """
    if IS_WINDOWS:
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        app_dir = os.path.join(base, "SyncVault")
        os.makedirs(app_dir, exist_ok=True)
        return app_dir
    else:
        return os.path.expanduser("~")


_APP_DATA_DIR = _get_app_data_dir()

# -----------------------------------------------------------------------
# ロギング設定（ログファイルをプラットフォーム対応パスに）
# -----------------------------------------------------------------------
_LOG_PATH = os.path.join(_APP_DATA_DIR, "backup.log") if IS_WINDOWS else "backup.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(_LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("backup")

# -----------------------------------------------------------------------
# デフォルトパス（プラットフォーム別）
# -----------------------------------------------------------------------
# Windows : %APPDATA%\SyncVault\config.json
# Linux   : ~/.backup_config.json
DEFAULT_CONFIG_PATH = (
    os.path.join(_APP_DATA_DIR, "config.json")
    if IS_WINDOWS
    else os.path.expanduser("~/.backup_config.json")
)

# Windows : %APPDATA%\SyncVault\catalog.db
# Linux   : ~/.backup_catalog.db
DEFAULT_CATALOG_PATH = (
    os.path.join(_APP_DATA_DIR, "catalog.db")
    if IS_WINDOWS
    else os.path.expanduser("~/.backup_catalog.db")
)

# 並列処理の設定
DEFAULT_MAX_WORKERS = min(32, (os.cpu_count() or 1) * 2)
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for file reading


# -----------------------------------------------------------------------
# プラットフォームヘルパー
# -----------------------------------------------------------------------
class PlatformHelper:
    """プラットフォーム固有の挙動を集約するユーティリティクラス。"""

    @staticmethod
    def get_default_compression_format() -> str:
        """デフォルト圧縮形式を返す。

        * Windows: zip（標準エクスプローラーで開ける）
        * Linux  : tar.gz（Unix 系ツールとの親和性が高い）
        """
        return "zip" if IS_WINDOWS else "tar.gz"

    @staticmethod
    def get_default_exclude_patterns() -> List[str]:
        """OS ごとの不要ファイル除外パターンを返す。"""
        common = ["*.tmp", "*.temp", "~*"]
        if IS_WINDOWS:
            return common + ["Thumbs.db", "desktop.ini", "*.lnk"]
        else:
            return common + [".DS_Store", "*.swp", "*.swo"]

    @staticmethod
    def normalize_archive_path(path: str) -> str:
        """アーカイブ内エントリのパスを常に '/' 区切りに正規化する。

        ZIP / tar.gz は内部的に '/' を使うため、Windows 環境で生成した
        バックスラッシュ区切りのパスを変換する。
        """
        return path.replace("\\", "/")

    @staticmethod
    def set_file_permissions(filepath: str, mode: int = 0o600) -> None:
        """ファイルのパーミッションを設定する（Linux / macOS のみ有効）。

        Windows では chmod の概念が異なるためスキップする。
        """
        if not IS_WINDOWS:
            try:
                os.chmod(filepath, mode)
            except OSError as e:
                logger.debug(f"パーミッション設定をスキップしました: {filepath} - {e}")

    @staticmethod
    def platform_name() -> str:
        """現在の OS 名を返す。"""
        return _SYSTEM or "Unknown"

class SFTPBackend:
    """SFTP/SSH バックエンドクラス。paramiko を使用してリモートサーバーへの
    バックアップ転送・取得を行う。"""

    def __init__(self, sftp_config: dict):
        self.host = sftp_config["host"]
        self.port = int(sftp_config.get("port", 22))
        self.username = sftp_config["username"]
        self.key_file = os.path.expanduser(sftp_config["key_file"])
        self.remote_path = sftp_config.get("remote_path", "/")
        self.known_hosts_file = os.path.expanduser(
            sftp_config.get("known_hosts_file", "~/.syncvault_known_hosts")
        )
        self._ssh = None
        self._sftp = None

    def _verify_or_register_host_key(self) -> None:
        """ホスト鍵を検証または初回登録する。

        * 初回接続: ホスト鍵を known_hosts_file に自動登録する
        * 2回目以降: 保存済み鍵と一致するか検証し、不一致の場合は例外を送出する
        """
        import paramiko

        # 認証前にサーバーの公開鍵を取得
        transport = paramiko.Transport((self.host, self.port))
        try:
            transport.start_client(timeout=10)
            server_key = transport.get_remote_server_key()
        finally:
            transport.close()

        known_hosts = paramiko.HostKeys()
        if os.path.exists(self.known_hosts_file):
            known_hosts.load(self.known_hosts_file)

        hostname = self.host if self.port == 22 else f"[{self.host}]:{self.port}"
        existing = known_hosts.lookup(hostname)

        if existing is None:
            # 初回接続: 鍵を登録して保存
            known_hosts.add(hostname, server_key.get_name(), server_key)
            kh_dir = os.path.dirname(os.path.abspath(self.known_hosts_file))
            os.makedirs(kh_dir, exist_ok=True)
            known_hosts.save(self.known_hosts_file)
            PlatformHelper.set_file_permissions(self.known_hosts_file)
            logger.info(
                f"ホスト鍵を登録しました: {hostname} ({server_key.get_name()})"
            )
        else:
            # 2回目以降: 保存済み鍵と比較
            stored_key = existing.get(server_key.get_name())
            if stored_key is None or stored_key.asbytes() != server_key.asbytes():
                raise ValueError(
                    f"ホスト鍵の検証に失敗しました: {hostname}\n"
                    "中間者攻撃の可能性があります。\n"
                    f"正当なサーバー変更の場合は {self.known_hosts_file} から"
                    "該当エントリを削除してください。"
                )
            logger.debug(f"ホスト鍵の検証成功: {hostname}")

    def connect(self) -> None:
        """SFTP サーバーに SSH 鍵認証で接続する。"""
        import paramiko

        self._verify_or_register_host_key()

        self._ssh = paramiko.SSHClient()
        self._ssh.load_host_keys(self.known_hosts_file)
        self._ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
        self._ssh.connect(
            self.host,
            port=self.port,
            username=self.username,
            key_filename=self.key_file,
            look_for_keys=False,
            allow_agent=True,
        )
        self._sftp = self._ssh.open_sftp()
        logger.info(f"SFTP接続完了: {self.username}@{self.host}:{self.port}")

    def disconnect(self) -> None:
        """SFTP 接続を閉じる。"""
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._ssh:
            self._ssh.close()
            self._ssh = None
        logger.debug("SFTP接続を閉じました")

    def makedirs(self, remote_dir: str) -> None:
        """リモートディレクトリを再帰的に作成する。"""
        if not remote_dir or remote_dir == "/":
            return
        parent = posixpath.dirname(remote_dir)
        if parent and parent != remote_dir:
            self.makedirs(parent)
        try:
            self._sftp.stat(remote_dir)
        except FileNotFoundError:
            self._sftp.mkdir(remote_dir)

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """ファイルをアトミックにアップロードする。

        一時ファイル名（remote_path + ".tmp"）でアップロードし、
        完了後にリネームすることで中断時に不完全ファイルが残らない。
        """
        remote_dir = posixpath.dirname(remote_path)
        if remote_dir:
            self.makedirs(remote_dir)
        tmp_path = remote_path + ".tmp"
        try:
            self._sftp.put(local_path, tmp_path)
            try:
                self._sftp.remove(remote_path)
            except FileNotFoundError:
                pass
            self._sftp.rename(tmp_path, remote_path)
            logger.debug(f"アップロード完了: {remote_path}")
        except Exception:
            try:
                self._sftp.remove(tmp_path)
            except Exception:
                pass
            raise

    def download_file(self, remote_path: str, local_path: str) -> None:
        """リモートファイルをローカルにダウンロードする。"""
        self._sftp.get(remote_path, local_path)
        logger.debug(f"ダウンロード完了: {remote_path} -> {local_path}")

    def download_dir(self, remote_dir: str, local_dir: str) -> None:
        """リモートディレクトリをローカルに再帰的にダウンロードする。"""
        import stat as stat_module
        os.makedirs(local_dir, exist_ok=True)
        for attr in self._sftp.listdir_attr(remote_dir):
            remote_item = posixpath.join(remote_dir, attr.filename)
            local_item = os.path.join(local_dir, attr.filename)
            if attr.st_mode and stat_module.S_ISDIR(attr.st_mode):
                self.download_dir(remote_item, local_item)
            else:
                self._sftp.get(remote_item, local_item)

    def list_remote_backups(self, remote_dir: str) -> List[str]:
        """リモートディレクトリ内のバックアップ一覧を mtime 昇順で返す。

        マニフェストファイル（.manifest.json）と一時ファイル（.tmp）は除外する。
        """
        try:
            attrs = self._sftp.listdir_attr(remote_dir)
        except FileNotFoundError:
            return []
        backups = [
            a for a in attrs
            if a.filename.startswith("backup_")
            and not a.filename.endswith(".tmp")
            and not a.filename.endswith(".manifest.json")
        ]
        backups.sort(key=lambda a: a.st_mtime or 0)
        return [posixpath.join(remote_dir, a.filename) for a in backups]

    def delete_remote_file(self, remote_path: str) -> None:
        """リモートファイルを削除する。存在しない場合は警告のみ。"""
        try:
            self._sftp.remove(remote_path)
            logger.info(f"リモートファイルを削除しました: {remote_path}")
        except FileNotFoundError:
            logger.warning(f"削除対象のファイルが見つかりません: {remote_path}")


class BackupCatalog:
    """SQLiteを使用してバックアップカタログを管理するクラス"""

    def __init__(self, db_path: str = DEFAULT_CATALOG_PATH):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        """データベースとテーブルを初期化する"""
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS backups (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp       TEXT NOT NULL,
                    backup_type     TEXT NOT NULL,
                    backup_path     TEXT NOT NULL,
                    manifest_path   TEXT,
                    total_size      INTEGER DEFAULT 0,
                    file_count      INTEGER DEFAULT 0,
                    processed       INTEGER DEFAULT 0,
                    skipped         INTEGER DEFAULT 0,
                    errors          INTEGER DEFAULT 0,
                    elapsed_time    REAL DEFAULT 0.0
                );

                CREATE TABLE IF NOT EXISTS catalog_files (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    backup_id   INTEGER NOT NULL REFERENCES backups(id) ON DELETE CASCADE,
                    rel_path    TEXT NOT NULL,
                    source_path TEXT NOT NULL,
                    file_hash   TEXT NOT NULL,
                    file_size   INTEGER DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_catalog_files_backup_id
                    ON catalog_files(backup_id);
                CREATE INDEX IF NOT EXISTS idx_catalog_files_rel_path
                    ON catalog_files(rel_path);
            """)

    def register_backup(
        self,
        backup_record: Dict[str, Any],
        manifest: Dict[str, str],
        file_sizes: Dict[str, int],
        source_paths: Dict[str, str],
    ) -> int:
        """バックアップセッションとファイル一覧をカタログに登録する

        Args:
            backup_record: run_backup で構築したバックアップ情報辞書
            manifest: {rel_path: file_hash} のマッピング
            file_sizes: {rel_path: file_size_bytes} のマッピング
            source_paths: {rel_path: absolute_source_path} のマッピング

        Returns:
            登録されたバックアップの id
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO backups
                    (timestamp, backup_type, backup_path, manifest_path,
                     total_size, file_count, processed, skipped, errors, elapsed_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    backup_record["timestamp"],
                    backup_record["type"],
                    backup_record["path"],
                    backup_record.get("manifest_path"),
                    backup_record.get("size", 0),
                    backup_record.get("file_count", 0),
                    backup_record.get("processed", 0),
                    backup_record.get("skipped", 0),
                    backup_record.get("errors", 0),
                    backup_record.get("elapsed_time", 0.0),
                ),
            )
            backup_id = cur.lastrowid

            rows = [
                (
                    backup_id,
                    rel_path,
                    source_paths.get(rel_path, ""),
                    file_hash,
                    file_sizes.get(rel_path, 0),
                )
                for rel_path, file_hash in manifest.items()
            ]
            conn.executemany(
                """
                INSERT INTO catalog_files
                    (backup_id, rel_path, source_path, file_hash, file_size)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
        return backup_id

    def list_backups(self) -> List[Dict[str, Any]]:
        """登録されているバックアップ一覧を返す"""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM backups ORDER BY timestamp DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_backup_files(self, backup_id: int) -> List[Dict[str, Any]]:
        """指定バックアップに含まれるファイル一覧を返す

        Args:
            backup_id: バックアップ ID

        Returns:
            ファイル情報の辞書リスト
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM catalog_files WHERE backup_id = ? ORDER BY rel_path",
                (backup_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def find_file_backups(self, pattern: str) -> List[Dict[str, Any]]:
        """ファイル名パターンに一致するファイルが含まれるバックアップを検索する

        Args:
            pattern: ファイルの相対パスに対する SQL LIKE パターン（例: '%.py'）

        Returns:
            マッチしたファイルとそのバックアップ情報の辞書リスト
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    b.id            AS backup_id,
                    b.timestamp,
                    b.backup_type,
                    b.backup_path,
                    f.rel_path,
                    f.source_path,
                    f.file_hash,
                    f.file_size
                FROM catalog_files f
                JOIN backups b ON b.id = f.backup_id
                WHERE f.rel_path LIKE ?
                ORDER BY b.timestamp DESC, f.rel_path
                """,
                (pattern,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_file_history(self, rel_path: str) -> List[Dict[str, Any]]:
        """特定ファイルのバックアップ履歴を返す

        Args:
            rel_path: カタログ内の相対パス（完全一致）

        Returns:
            バックアップ履歴の辞書リスト（新しい順）
        """
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    b.id            AS backup_id,
                    b.timestamp,
                    b.backup_type,
                    b.backup_path,
                    f.file_hash,
                    f.file_size
                FROM catalog_files f
                JOIN backups b ON b.id = f.backup_id
                WHERE f.rel_path = ?
                ORDER BY b.timestamp DESC
                """,
                (rel_path,),
            ).fetchall()
        return [dict(r) for r in rows]

    def delete_backup(self, backup_id: int) -> None:
        """バックアップをカタログから削除する（関連ファイルも CASCADE 削除）。"""
        with self._connect() as conn:
            conn.execute("DELETE FROM backups WHERE id = ?", (backup_id,))


class FileProcessor:
    """ファイル処理用のヘルパークラス"""
    
    def __init__(self, temp_dir: str, source_base_paths: List[str]):
        self.temp_dir = temp_dir
        self.source_base_paths = source_base_paths
        self.processed_count = 0
        self.skipped_count = 0
        self.error_count = 0
        self.total_size = 0
        self.lock = Lock()
        
    def update_stats(self, processed: int = 0, skipped: int = 0, 
                    error: int = 0, size: int = 0):
        """統計情報を更新する（スレッドセーフ）"""
        with self.lock:
            self.processed_count += processed
            self.skipped_count += skipped
            self.error_count += error
            self.total_size += size
            
    def get_stats(self) -> Dict[str, int]:
        """統計情報を取得する"""
        with self.lock:
            return {
                "processed": self.processed_count,
                "skipped": self.skipped_count,
                "errors": self.error_count,
                "total_size": self.total_size
            }

class BackupManager:
    """バックアップ処理を管理するクラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH,
                 catalog_path: str = DEFAULT_CATALOG_PATH):
        """初期化処理

        Args:
            config_path: 設定ファイルのパス
            catalog_path: SQLite カタログ DB のパス
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.max_workers = self.config.get("max_workers", DEFAULT_MAX_WORKERS)
        self.catalog = BackupCatalog(catalog_path)
        
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイルを読み込む
        
        Returns:
            設定データを含む辞書
        """
        if not os.path.exists(self.config_path):
            # デフォルト設定（プラットフォームに応じて自動調整）
            default_config = {
                "sources": [],
                "destination": "",
                "destination_type": "local",  # "local" または "sftp"
                "sftp": {
                    "host": "",
                    "port": 22,
                    "username": "",
                    "key_file": "",
                    "remote_path": "",
                    "known_hosts_file": "~/.syncvault_known_hosts",
                    "max_generations": 0,
                },
                "backup_type": "full",  # full または differential
                "compress": True,
                "compression_format": PlatformHelper.get_default_compression_format(),
                "schedule": {
                    "type": "daily",  # daily, weekly, monthly
                    "time": "00:00",
                    "day_of_week": 0,  # 0 = 月曜日
                    "full_backup_day": 0  # 0 = 月曜日
                },
                "history": [],
                "max_workers": DEFAULT_MAX_WORKERS,  # 並列処理の最大ワーカー数
                "exclude_patterns": PlatformHelper.get_default_exclude_patterns(),
            }

            # デフォルト設定を保存
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4)
            # Linux では設定ファイルのパーミッションを制限する
            PlatformHelper.set_file_permissions(self.config_path)

            return default_config
        
        # 既存の設定ファイルを読み込む
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 新しい設定項目がない場合はデフォルト値を追加
                if "max_workers" not in config:
                    config["max_workers"] = DEFAULT_MAX_WORKERS
                if "exclude_patterns" not in config:
                    config["exclude_patterns"] = PlatformHelper.get_default_exclude_patterns()
                if "compression_format" not in config:
                    config["compression_format"] = PlatformHelper.get_default_compression_format()
                if "destination_type" not in config:
                    config["destination_type"] = "local"
                if "sftp" not in config:
                    config["sftp"] = {
                        "host": "", "port": 22, "username": "", "key_file": "",
                        "remote_path": "", "known_hosts_file": "~/.syncvault_known_hosts",
                        "max_generations": 0,
                    }
                return config
        except json.JSONDecodeError:
            logger.error("設定ファイルの形式が不正です")
            return {}
    
    def _save_config(self) -> None:
        """設定ファイルを保存する"""
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.config, f, indent=4)
    
    def add_source(self, path: str) -> None:
        """バックアップ元を追加する
        
        Args:
            path: バックアップ元のパス
        """
        if os.path.exists(path):
            if path not in self.config["sources"]:
                self.config["sources"].append(path)
                self._save_config()
                logger.info(f"バックアップ元を追加しました: {path}")
            else:
                logger.warning(f"バックアップ元は既に追加されています: {path}")
        else:
            logger.error(f"指定されたパスが存在しません: {path}")
    
    def remove_source(self, path: str) -> None:
        """バックアップ元を削除する
        
        Args:
            path: バックアップ元のパス
        """
        if path in self.config["sources"]:
            self.config["sources"].remove(path)
            self._save_config()
            logger.info(f"バックアップ元を削除しました: {path}")
        else:
            logger.warning(f"指定されたバックアップ元は登録されていません: {path}")
    
    def set_destination(self, path: str) -> None:
        """バックアップ先を設定する
        
        Args:
            path: バックアップ先のパス
        """
        # パスの存在確認（ネットワークドライブの場合は失敗する可能性あり）
        try:
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
                
            self.config["destination"] = path
            self._save_config()
            logger.info(f"バックアップ先を設定しました: {path}")
        except Exception as e:
            logger.error(f"バックアップ先の設定に失敗しました: {str(e)}")
    
    def set_backup_type(self, backup_type: str) -> None:
        """バックアップの種類を設定する
        
        Args:
            backup_type: 'full' または 'differential'
        """
        if backup_type in ["full", "differential"]:
            self.config["backup_type"] = backup_type
            self._save_config()
            logger.info(f"バックアップの種類を設定しました: {backup_type}")
        else:
            logger.error(f"不正なバックアップ種類です: {backup_type}")
    
    def set_compression(self, compress: bool, format_type: str = None) -> None:
        """圧縮設定を行う
        
        Args:
            compress: 圧縮するかどうか
            format_type: 圧縮形式 ('zip' または 'tar.gz')
        """
        self.config["compress"] = compress
        
        if format_type and format_type in ["zip", "tar.gz"]:
            self.config["compression_format"] = format_type
            
        self._save_config()
        logger.info(f"圧縮設定を更新しました: 圧縮={compress}, 形式={self.config['compression_format']}")
    
    def set_max_workers(self, max_workers: int) -> None:
        """最大ワーカー数を設定する
        
        Args:
            max_workers: 最大ワーカー数
        """
        if max_workers > 0:
            self.config["max_workers"] = max_workers
            self.max_workers = max_workers
            self._save_config()
            logger.info(f"最大ワーカー数を設定しました: {max_workers}")
        else:
            logger.error("最大ワーカー数は1以上である必要があります")
    
    def set_schedule(self, schedule_type: str, time_str: str = None, 
                    day_of_week: int = None, full_backup_day: int = None) -> None:
        """スケジュール設定を行う
        
        Args:
            schedule_type: スケジュールの種類 ('daily', 'weekly', 'monthly')
            time_str: 実行時刻 (例: '00:00')
            day_of_week: 実行曜日 (0-6, 0=月曜日)
            full_backup_day: 完全バックアップを行う曜日 (0-6, 0=月曜日)
        """
        if schedule_type not in ["daily", "weekly", "monthly"]:
            logger.error(f"不正なスケジュール種類です: {schedule_type}")
            return
            
        self.config["schedule"]["type"] = schedule_type
        
        if time_str:
            # 時刻形式の検証
            try:
                datetime.datetime.strptime(time_str, "%H:%M")
                self.config["schedule"]["time"] = time_str
            except ValueError:
                logger.error(f"不正な時刻形式です: {time_str}")
        
        if day_of_week is not None and 0 <= day_of_week <= 6:
            self.config["schedule"]["day_of_week"] = day_of_week
        
        if full_backup_day is not None and 0 <= full_backup_day <= 6:
            self.config["schedule"]["full_backup_day"] = full_backup_day
            
        self._save_config()
        logger.info(f"スケジュール設定を更新しました: {self.config['schedule']}")
    
    def should_run_full_backup(self) -> bool:
        """完全バックアップを実行すべきかどうかを判断する
        
        Returns:
            完全バックアップを実行すべき場合はTrue
        """
        # 履歴がない場合は必ず完全バックアップ
        if not self.config["history"]:
            return True
            
        # バックアップタイプが 'full' の場合は常に完全バックアップ
        if self.config["backup_type"] == "full":
            return True
            
        # 今日の曜日
        today_weekday = datetime.datetime.now().weekday()
        
        # 完全バックアップの曜日と一致する場合
        if today_weekday == self.config["schedule"]["full_backup_day"]:
            return True
            
        return False
    
    def _get_file_hash(self, filepath: str) -> str:
        """ファイルのハッシュ値を計算する（メモリ効率的）
        
        Args:
            filepath: ファイルパス
            
        Returns:
            ファイルのMD5ハッシュ値
        """
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                while chunk := f.read(CHUNK_SIZE):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"ハッシュ計算エラー: {filepath} - {str(e)}")
            raise
    
    def _should_exclude(self, filepath: str) -> bool:
        """ファイルを除外すべきかどうかを判断する
        
        Args:
            filepath: ファイルパス
            
        Returns:
            除外すべき場合はTrue
        """
        filename = os.path.basename(filepath)
        for pattern in self.config.get("exclude_patterns", []):
            if fnmatch.fnmatch(filename, pattern):
                return True
        return False
    
    def _collect_files(self, sources: List[str]) -> List[Tuple[str, str]]:
        """バックアップ対象のファイルを収集する
        
        Args:
            sources: バックアップ元のパスリスト
            
        Returns:
            (絶対パス, 相対パス)のタプルのリスト
        """
        files = []
        
        for source_path in sources:
            if not os.path.exists(source_path):
                logger.warning(f"バックアップ元が存在しません: {source_path}")
                continue
                
            if os.path.isfile(source_path):
                if not self._should_exclude(source_path):
                    files.append((source_path, os.path.basename(source_path)))
            else:
                for root, _, filenames in os.walk(source_path):
                    for filename in filenames:
                        file_path = os.path.join(root, filename)
                        if not self._should_exclude(file_path):
                            rel_path = os.path.relpath(file_path, os.path.dirname(source_path))
                            files.append((file_path, rel_path))
                            
        return files
    
    def _process_file(self, file_info: Tuple[str, str], temp_dir: str,
                     last_manifest: Dict[str, str], is_full_backup: bool,
                     file_processor: FileProcessor) -> Optional[Tuple[str, str, str, int]]:
        """単一ファイルを処理する

        Args:
            file_info: (絶対パス, 相対パス)のタプル
            temp_dir: 一時ディレクトリ
            last_manifest: 前回のマニフェスト
            is_full_backup: 完全バックアップかどうか
            file_processor: FileProcessorインスタンス

        Returns:
            成功時は(相対パス, ハッシュ値, 絶対パス, ファイルサイズ)のタプル、
            スキップ/エラー時はNone
        """
        file_path, rel_path = file_info

        try:
            # ファイルのハッシュ値を計算
            file_hash = self._get_file_hash(file_path)

            # 差分バックアップの場合はチェック
            if not is_full_backup and rel_path in last_manifest and last_manifest[rel_path] == file_hash:
                logger.debug(f"変更なしのためスキップします: {file_path}")
                file_processor.update_stats(skipped=1)
                return None

            # 出力先のディレクトリを作成
            dest_dir = os.path.join(temp_dir, os.path.dirname(rel_path))
            os.makedirs(dest_dir, exist_ok=True)

            # ファイルをコピー
            dest_file = os.path.join(temp_dir, rel_path)
            shutil.copy2(file_path, dest_file)

            # ファイルサイズを取得
            file_size = os.path.getsize(file_path)
            file_processor.update_stats(processed=1, size=file_size)

            return (rel_path, file_hash, file_path, file_size)

        except Exception as e:
            logger.error(f"ファイル処理エラー: {file_path} - {str(e)}")
            file_processor.update_stats(error=1)
            return None
    
    def _get_last_backup_manifest(self) -> Dict[str, str]:
        """最後のバックアップのマニフェストを取得する。

        SFTP バックアップの場合はリモートからダウンロードして読み込む。

        Returns:
            ファイルパスとハッシュ値の辞書
        """
        if not self.config["history"]:
            return {}

        last_backup = self.config["history"][-1]
        manifest_path = last_backup.get("manifest_path")

        if not manifest_path:
            return {}

        # SFTP からマニフェストを取得
        if manifest_path.startswith("sftp://"):
            tmp_manifest = None
            backend = self._get_sftp_backend()
            try:
                backend.connect()
                remote_manifest = self._parse_sftp_remote_path(manifest_path)
                tmp_manifest = tempfile.mktemp(suffix=".manifest.json")
                backend.download_file(remote_manifest, tmp_manifest)
                with open(tmp_manifest, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"SFTP からマニフェストを取得できませんでした: {e}")
                return {}
            finally:
                backend.disconnect()
                if tmp_manifest and os.path.exists(tmp_manifest):
                    try:
                        os.remove(tmp_manifest)
                    except OSError:
                        pass

        # ローカルマニフェスト
        if not os.path.exists(manifest_path):
            return {}

        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            logger.error("マニフェストファイルの読み込みに失敗しました")
            return {}
    
    def _create_backup_filename(self, is_full: bool) -> str:
        """バックアップファイル名を生成する
        
        Args:
            is_full: 完全バックアップの場合はTrue
            
        Returns:
            バックアップファイル名
        """
        now = datetime.datetime.now()
        date_str = now.strftime("%Y%m%d_%H%M%S")
        backup_type = "full" if is_full else "diff"
        
        if self.config["compress"]:
            ext = ".zip" if self.config["compression_format"] == "zip" else ".tar.gz"
        else:
            ext = ""
            
        return f"backup_{date_str}_{backup_type}{ext}"
    
    def _compress_directory(self, source_dir: str, output_path: str) -> None:
        """ディレクトリを圧縮する

        Args:
            source_dir: 圧縮元ディレクトリ
            output_path: 出力ファイルパス
        """
        if self.config["compression_format"] == "zip":
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, source_dir)
                        zipf.write(file_path, arcname)
        else:  # tar.gz
            with tarfile.open(output_path, "w:gz") as tar:
                tar.add(source_dir, arcname=os.path.basename(source_dir))

    # -----------------------------------------------------------------------
    # SFTP ヘルパーメソッド
    # -----------------------------------------------------------------------

    def _get_sftp_backend(self) -> SFTPBackend:
        """設定から SFTPBackend インスタンスを生成する。"""
        return SFTPBackend(self.config.get("sftp", {}))

    def _make_sftp_url(self, remote_path: str) -> str:
        """リモートパスから sftp://host:port/path 形式の URL を生成する。"""
        sftp_cfg = self.config["sftp"]
        host = sftp_cfg["host"]
        port = sftp_cfg.get("port", 22)
        return f"sftp://{host}:{port}{remote_path}"

    def _parse_sftp_remote_path(self, sftp_url: str) -> str:
        """sftp://host:port/path から /path 部分を抽出する。"""
        without_scheme = sftp_url[len("sftp://"):]
        slash_idx = without_scheme.find("/")
        if slash_idx == -1:
            return "/"
        return without_scheme[slash_idx:]

    def set_sftp_destination(
        self,
        host: str,
        username: str,
        key_file: str,
        remote_path: str,
        port: int = 22,
        max_generations: int = 0,
    ) -> None:
        """SFTP バックアップ先を設定する。

        Args:
            host: SFTP サーバーのホスト名または IP アドレス
            username: SSH ログインユーザー名
            key_file: SSH 秘密鍵ファイルのパス
            remote_path: リモートサーバー上のバックアップ保存ディレクトリ
            port: SSH ポート番号（デフォルト 22）
            max_generations: 保持する世代数（0 = 無制限）
        """
        key_file_expanded = os.path.expanduser(key_file)
        if not os.path.exists(key_file_expanded):
            logger.error(f"SSH 鍵ファイルが見つかりません: {key_file}")
            return
        self.config["destination_type"] = "sftp"
        self.config["sftp"] = {
            "host": host,
            "port": port,
            "username": username,
            "key_file": key_file,
            "remote_path": remote_path,
            "known_hosts_file": "~/.syncvault_known_hosts",
            "max_generations": max_generations,
        }
        self._save_config()
        logger.info(
            f"SFTP バックアップ先を設定しました: {username}@{host}:{port}{remote_path}"
        )

    def test_sftp_connection(self) -> bool:
        """SFTP 接続と書き込み権限をテストする。

        Returns:
            接続成功の場合は True
        """
        if self.config.get("destination_type") != "sftp":
            logger.error(
                "SFTP が設定されていません。set-sftp-destination を実行してください"
            )
            return False
        backend = self._get_sftp_backend()
        try:
            backend.connect()
            sftp_cfg = self.config["sftp"]
            backend.makedirs(sftp_cfg["remote_path"])
            logger.info(
                f"SFTP 接続テスト成功: "
                f"{sftp_cfg['username']}@{sftp_cfg['host']}:{sftp_cfg.get('port', 22)}"
            )
            return True
        except Exception as e:
            logger.error(f"SFTP 接続テストに失敗しました: {e}")
            return False
        finally:
            backend.disconnect()

    def _prune_remote_generations(self, backend: SFTPBackend) -> None:
        """世代数を超えた古いリモートバックアップを削除する。

        max_generations が 0 以下の場合は何もしない。
        最新バックアップがカタログに登録済みの状態で呼び出すこと。
        """
        sftp_cfg = self.config.get("sftp", {})
        max_gen = sftp_cfg.get("max_generations", 0)
        if not max_gen or max_gen <= 0:
            return

        remote_dir = sftp_cfg["remote_path"]
        remote_backups = backend.list_remote_backups(remote_dir)
        excess_count = len(remote_backups) - max_gen
        if excess_count <= 0:
            return

        all_catalog_backups = self.catalog.list_backups()
        for remote_file in remote_backups[:excess_count]:
            # バックアップ本体を削除
            backend.delete_remote_file(remote_file)
            # マニフェストファイルも削除
            backend.delete_remote_file(remote_file + ".manifest.json")

            # カタログから削除
            sftp_url = self._make_sftp_url(remote_file)
            for b in all_catalog_backups:
                if b["backup_path"] == sftp_url:
                    self.catalog.delete_backup(b["id"])
                    break

            # config 履歴からも削除
            self.config["history"] = [
                h for h in self.config.get("history", [])
                if h.get("path") != sftp_url
            ]

        logger.info(f"{excess_count} 件の古いバックアップを削除しました")
        self._save_config()

    def _download_sftp_to_temp(self, sftp_url: str) -> Tuple[str, str]:
        """SFTP バックアップをローカル一時ディレクトリにダウンロードする。

        Returns:
            (local_path, tmp_dir) のタプル。
            tmp_dir は呼び出し元が shutil.rmtree で削除する責任を持つ。
        """
        remote_path = self._parse_sftp_remote_path(sftp_url)
        tmp_dir = tempfile.mkdtemp(prefix="syncvault_restore_")
        local_filename = posixpath.basename(remote_path)
        local_path = os.path.join(tmp_dir, local_filename)

        backend = self._get_sftp_backend()
        backend.connect()
        try:
            is_archive = sftp_url.endswith(".zip") or sftp_url.endswith(".tar.gz")
            if is_archive:
                backend.download_file(remote_path, local_path)
            else:
                backend.download_dir(remote_path, local_path)
            logger.info(f"バックアップをダウンロードしました: {sftp_url}")
        finally:
            backend.disconnect()

        return local_path, tmp_dir

    def run_backup(self) -> bool:
        """バックアップを実行する（並列処理版）
        
        Returns:
            バックアップが成功した場合はTrue
        """
        if not self.config["sources"]:
            logger.error("バックアップ元が設定されていません")
            return False

        destination_type = self.config.get("destination_type", "local")
        if destination_type == "sftp":
            sftp_cfg = self.config.get("sftp", {})
            if not sftp_cfg.get("host") or not sftp_cfg.get("username") or not sftp_cfg.get("key_file"):
                logger.error(
                    "SFTP接続先が設定されていません。set-sftp-destination を実行してください"
                )
                return False
        elif not self.config["destination"]:
            logger.error("バックアップ先が設定されていません")
            return False
            
        # バックアップの種類を判断
        is_full_backup = self.should_run_full_backup()
        backup_type = "完全" if is_full_backup else "差分"
        logger.info(f"{backup_type}バックアップを開始します")
        logger.info(f"並列処理ワーカー数: {self.max_workers}")
        
        # 一時ディレクトリを作成
        # OS 標準の一時領域（Windows: %TEMP%, Linux: /tmp など）を使用する
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_dir = os.path.join(tempfile.gettempdir(), f"syncvault_temp_{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # ファイルを収集
            logger.info("バックアップ対象ファイルを収集中...")
            files = self._collect_files(self.config["sources"])
            logger.info(f"対象ファイル数: {len(files)}")
            
            if not files:
                logger.warning("バックアップ対象のファイルがありません")
                return False
            
            # 前回のバックアップマニフェスト
            last_manifest = self._get_last_backup_manifest() if not is_full_backup else {}
            
            # FileProcessorインスタンスを作成
            file_processor = FileProcessor(temp_dir, self.config["sources"])
            
            # マニフェスト（ファイルとハッシュ値のマッピング）
            manifest = {}
            file_sizes: Dict[str, int] = {}
            source_paths: Dict[str, str] = {}
            manifest_lock = Lock()
            
            # 並列処理でファイルを処理
            logger.info("ファイル処理を開始します...")
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # プログレスバーの設定
                if TQDM_AVAILABLE:
                    pbar = tqdm(total=len(files), desc="処理中", unit="files")
                
                # ファイル処理タスクを投入
                futures = {
                    executor.submit(
                        self._process_file, 
                        file_info, 
                        temp_dir, 
                        last_manifest, 
                        is_full_backup,
                        file_processor
                    ): file_info for file_info in files
                }
                
                # 完了したタスクから結果を収集
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            rel_path, file_hash, src_path, fsize = result
                            with manifest_lock:
                                manifest[rel_path] = file_hash
                                file_sizes[rel_path] = fsize
                                source_paths[rel_path] = src_path
                                
                        if TQDM_AVAILABLE:
                            pbar.update(1)
                            stats = file_processor.get_stats()
                            pbar.set_postfix({
                                "処理": stats["processed"],
                                "スキップ": stats["skipped"],
                                "エラー": stats["errors"],
                                "サイズ": f"{stats['total_size'] / (1024*1024):.1f}MB"
                            })
                            
                    except Exception as e:
                        logger.error(f"ファイル処理中にエラーが発生しました: {str(e)}")
                
                if TQDM_AVAILABLE:
                    pbar.close()
            
            # 処理時間と統計を表示
            elapsed_time = time.time() - start_time
            stats = file_processor.get_stats()
            logger.info(f"ファイル処理完了 - 処理時間: {elapsed_time:.2f}秒")
            logger.info(f"処理済み: {stats['processed']}件, スキップ: {stats['skipped']}件, エラー: {stats['errors']}件")
            logger.info(f"総サイズ: {stats['total_size'] / (1024*1024):.2f}MB")
            
            if not manifest:
                logger.warning("バックアップするファイルがありません（すべてスキップまたはエラー）")
                return False
            
            # マニフェストファイルを一時ディレクトリに作成
            manifest_tmp_path = os.path.join(temp_dir, "manifest.json")
            with open(manifest_tmp_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4)

            # バックアップ先のファイル名
            backup_filename = self._create_backup_filename(is_full_backup)
            destination_type = self.config.get("destination_type", "local")

            if destination_type == "sftp":
                # ---- SFTP アップロード ----
                sftp_config = self.config["sftp"]
                remote_base = sftp_config["remote_path"]
                backend = self._get_sftp_backend()
                backend.connect()
                try:
                    if self.config["compress"]:
                        # ローカルに一時アーカイブを作成してアップロード
                        local_archive = os.path.join(
                            tempfile.gettempdir(), backup_filename
                        )
                        try:
                            logger.info("バックアップを圧縮中...")
                            self._compress_directory(temp_dir, local_archive)
                            remote_archive = posixpath.join(remote_base, backup_filename)
                            remote_manifest_sftp = remote_archive + ".manifest.json"
                            backend.upload_file(local_archive, remote_archive)
                            backend.upload_file(manifest_tmp_path, remote_manifest_sftp)
                            logger.info(
                                f"バックアップをアップロードしました: {remote_archive}"
                            )
                        finally:
                            if os.path.exists(local_archive):
                                os.remove(local_archive)
                    else:
                        # ディレクトリツリーをそのままアップロード
                        remote_archive = posixpath.join(remote_base, backup_filename)
                        remote_manifest_sftp = posixpath.join(
                            remote_archive, "manifest.json"
                        )
                        backend.makedirs(remote_archive)
                        for root, _dirs, fnames in os.walk(temp_dir):
                            for fname in fnames:
                                local_fp = os.path.join(root, fname)
                                rel = os.path.relpath(
                                    local_fp, temp_dir
                                ).replace("\\", "/")
                                backend.upload_file(
                                    local_fp, posixpath.join(remote_archive, rel)
                                )
                        logger.info(
                            f"バックアップをアップロードしました: {remote_archive}"
                        )

                    backup_path = self._make_sftp_url(remote_archive)
                    manifest_path = self._make_sftp_url(remote_manifest_sftp)
                    backup_size = 0

                    # 履歴とカタログに登録（世代管理の前に実施）
                    backup_record = {
                        "timestamp": timestamp,
                        "type": "full" if is_full_backup else "differential",
                        "path": backup_path,
                        "manifest_path": manifest_path,
                        "size": backup_size,
                        "file_count": len(manifest),
                        "processed": stats["processed"],
                        "skipped": stats["skipped"],
                        "errors": stats["errors"],
                        "elapsed_time": elapsed_time,
                    }
                    self.config["history"].append(backup_record)
                    self._save_config()
                    try:
                        self.catalog.register_backup(
                            backup_record, manifest, file_sizes, source_paths
                        )
                        logger.info("バックアップカタログを更新しました")
                    except Exception as e:
                        logger.warning(
                            f"カタログ更新に失敗しました（バックアップ自体は成功）: {e}"
                        )

                    # 世代管理（最新バックアップ登録後に実施）
                    self._prune_remote_generations(backend)
                finally:
                    backend.disconnect()
            else:
                # ---- ローカルファイルシステム ----
                backup_path = os.path.join(self.config["destination"], backup_filename)

                if self.config["compress"]:
                    logger.info("バックアップを圧縮中...")
                    self._compress_directory(temp_dir, backup_path)
                    logger.info(f"バックアップを圧縮しました: {backup_path}")
                    manifest_path = backup_path + ".manifest.json"
                    shutil.copy2(manifest_tmp_path, manifest_path)
                else:
                    shutil.copytree(temp_dir, backup_path)
                    logger.info(f"バックアップを作成しました: {backup_path}")
                    manifest_path = os.path.join(backup_path, "manifest.json")

                backup_record = {
                    "timestamp": timestamp,
                    "type": "full" if is_full_backup else "differential",
                    "path": backup_path,
                    "manifest_path": manifest_path,
                    "size": os.path.getsize(backup_path) if os.path.exists(backup_path) else 0,
                    "file_count": len(manifest),
                    "processed": stats["processed"],
                    "skipped": stats["skipped"],
                    "errors": stats["errors"],
                    "elapsed_time": elapsed_time,
                }
                self.config["history"].append(backup_record)
                self._save_config()
                try:
                    self.catalog.register_backup(
                        backup_record, manifest, file_sizes, source_paths
                    )
                    logger.info("バックアップカタログを更新しました")
                except Exception as e:
                    logger.warning(
                        f"カタログ更新に失敗しました（バックアップ自体は成功）: {e}"
                    )

            return True
            
        except Exception as e:
            logger.error(f"バックアップ中にエラーが発生しました: {str(e)}")
            return False
            
        finally:
            # 一時ディレクトリを削除（圧縮の有無にかかわらず常に削除）
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def restore_backup(self, backup_path: str, restore_path: str,
                       dry_run: bool = False) -> bool:
        """バックアップを復元する

        Args:
            backup_path: 復元するバックアップファイルまたはディレクトリのパス
                         sftp://host:port/path 形式の SFTP URL も指定可能
            restore_path: 復元先ディレクトリのパス
            dry_run: True の場合は実際のコピーを行わず、復元対象ファイルを表示するのみ

        Returns:
            復元が成功した場合は True
        """
        tmp_dir = None
        if backup_path.startswith("sftp://"):
            if dry_run:
                print(f"[ドライラン] SFTP からバックアップをダウンロードします: {backup_path}")
            try:
                backup_path, tmp_dir = self._download_sftp_to_temp(backup_path)
            except Exception as e:
                logger.error(f"SFTP からのダウンロードに失敗しました: {e}")
                return False

        try:
            if not os.path.exists(backup_path):
                logger.error(f"バックアップファイルが見つかりません: {backup_path}")
                return False

        except Exception:
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
            raise

        try:
            if dry_run:
                print(f"[ドライラン] 復元元: {backup_path}")
                print(f"[ドライラン] 復元先: {restore_path}")
                if os.path.isdir(backup_path):
                    for root, _, files in os.walk(backup_path):
                        for file in files:
                            src = os.path.join(root, file)
                            rel = os.path.relpath(src, backup_path)
                            print(f"  {rel}")
                elif backup_path.endswith('.zip'):
                    with zipfile.ZipFile(backup_path, 'r') as zipf:
                        for name in zipf.namelist():
                            print(f"  {name}")
                elif backup_path.endswith('.tar.gz'):
                    with tarfile.open(backup_path, 'r:gz') as tar:
                        for name in tar.getnames():
                            print(f"  {name}")
                return True

            os.makedirs(restore_path, exist_ok=True)

            if os.path.isdir(backup_path):
                # 圧縮なしバックアップの場合はディレクトリをコピー
                for root, dirs, files in os.walk(backup_path):
                    rel_root = os.path.relpath(root, backup_path)
                    dest_root = os.path.join(restore_path, rel_root)
                    os.makedirs(dest_root, exist_ok=True)
                    for file in files:
                        src_file = os.path.join(root, file)
                        dest_file = os.path.join(dest_root, file)
                        shutil.copy2(src_file, dest_file)
            elif backup_path.endswith('.zip'):
                with zipfile.ZipFile(backup_path, 'r') as zipf:
                    zipf.extractall(restore_path)
            elif backup_path.endswith('.tar.gz'):
                with tarfile.open(backup_path, 'r:gz') as tar:
                    tar.extractall(restore_path)
            else:
                logger.error(f"サポートされていないバックアップ形式です: {backup_path}")
                return False

            logger.info(f"バックアップを復元しました: {restore_path}")
            return True
        except Exception as e:
            logger.error(f"バックアップの復元に失敗しました: {str(e)}")
            return False
        finally:
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

    def get_latest_backup_id(self) -> Optional[int]:
        """カタログに登録された最新バックアップの ID を返す。

        Returns:
            最新バックアップの ID、またはバックアップが存在しない場合は None
        """
        backups = self.catalog.list_backups()
        if not backups:
            return None
        return backups[0]["id"]

    def restore_file(self, rel_path: str, backup_id: int, restore_path: str) -> bool:
        """カタログを使って特定のファイルだけを復元する

        Args:
            rel_path: カタログ内の相対パス（catalog-history で確認できる値）
            backup_id: バックアップ ID（catalog-search / catalog-history で確認できる値）
            restore_path: ファイルの復元先ディレクトリ

        Returns:
            成功した場合は True
        """
        # カタログからバックアップ情報を取得
        backups = self.catalog.list_backups()
        target_backup = next((b for b in backups if b["id"] == backup_id), None)
        if not target_backup:
            logger.error(f"バックアップ ID {backup_id} が見つかりません")
            return False

        backup_path = target_backup["backup_path"]

        tmp_dir = None
        if backup_path.startswith("sftp://"):
            try:
                backup_path, tmp_dir = self._download_sftp_to_temp(backup_path)
            except Exception as e:
                logger.error(f"SFTP からのダウンロードに失敗しました: {e}")
                return False

        if not os.path.exists(backup_path):
            logger.error(f"バックアップファイルが見つかりません: {backup_path}")
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)
            return False

        os.makedirs(restore_path, exist_ok=True)
        dest_file = os.path.join(restore_path, os.path.basename(rel_path))

        try:
            if os.path.isdir(backup_path):
                src = os.path.join(backup_path, rel_path)
                if not os.path.exists(src):
                    logger.error(f"バックアップ内にファイルが見つかりません: {rel_path}")
                    return False
                shutil.copy2(src, dest_file)

            elif backup_path.endswith(".zip"):
                with zipfile.ZipFile(backup_path, "r") as zipf:
                    # ZIP 内のパス区切りは常に '/' なので正規化する
                    zip_path = PlatformHelper.normalize_archive_path(rel_path)
                    if zip_path not in zipf.namelist():
                        logger.error(f"ZIP 内にファイルが見つかりません: {zip_path}")
                        return False
                    with zipf.open(zip_path) as src_f, open(dest_file, "wb") as dst_f:
                        shutil.copyfileobj(src_f, dst_f)

            elif backup_path.endswith(".tar.gz"):
                with tarfile.open(backup_path, "r:gz") as tar:
                    # tar 内のパスはアーカイブ名のプレフィックスが付く場合がある
                    # Windows バックスラッシュを '/' に正規化して比較する
                    members = tar.getnames()
                    norm_rel = PlatformHelper.normalize_archive_path(rel_path)
                    match = next(
                        (m for m in members if m == norm_rel or m.endswith("/" + norm_rel)),
                        None,
                    )
                    if match is None:
                        logger.error(f"tar.gz 内にファイルが見つかりません: {rel_path}")
                        return False
                    member = tar.getmember(match)
                    with tar.extractfile(member) as src_f, open(dest_file, "wb") as dst_f:
                        shutil.copyfileobj(src_f, dst_f)

            else:
                logger.error(f"サポートされていないバックアップ形式です: {backup_path}")
                return False

            logger.info(f"ファイルを復元しました: {rel_path} -> {dest_file}")
            return True

        except Exception as e:
            logger.error(f"ファイル復元に失敗しました: {e}")
            return False
        finally:
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

    def list_backups(self) -> List[Dict[str, Any]]:
        """バックアップの履歴を取得する

        Returns:
            バックアップ履歴のリスト
        """
        return self.config["history"]


def create_parser() -> argparse.ArgumentParser:
    """コマンドライン引数のパーサーを作成する
    
    Returns:
        ArgumentParser オブジェクト
    """
    parser = argparse.ArgumentParser(description='クロスプラットフォームバックアッププログラム（並列処理対応版）')
    
    subparsers = parser.add_subparsers(dest='command', help='コマンド')
    
    # バックアップ実行コマンド
    run_parser = subparsers.add_parser('run', help='バックアップを実行する')

    # バックアップ復元コマンド
    restore_parser = subparsers.add_parser('restore', help='バックアップを復元する')
    restore_parser.add_argument('destination', help='復元先のパス')
    restore_source_group = restore_parser.add_mutually_exclusive_group()
    restore_source_group.add_argument('--path', metavar='BACKUP_PATH',
                                      help='復元するバックアップファイルまたはディレクトリのパス')
    restore_source_group.add_argument('--id', type=int, metavar='CATALOG_ID',
                                      help='カタログ ID でバックアップを指定 (list コマンドで確認)')
    restore_source_group.add_argument('--latest', action='store_true',
                                      help='最新のバックアップを復元する')
    restore_parser.add_argument('--dry-run', action='store_true',
                                help='実際のコピーは行わず、復元対象ファイルのみ表示する')
    
    # バックアップ元追加コマンド
    add_source_parser = subparsers.add_parser('add-source', help='バックアップ元を追加する')
    add_source_parser.add_argument('path', help='バックアップ元のパス')
    
    # バックアップ元削除コマンド
    remove_source_parser = subparsers.add_parser('remove-source', help='バックアップ元を削除する')
    remove_source_parser.add_argument('path', help='バックアップ元のパス')
    
    # バックアップ先設定コマンド
    set_dest_parser = subparsers.add_parser('set-destination', help='バックアップ先を設定する')
    set_dest_parser.add_argument('path', help='バックアップ先のパス')
    
    # バックアップタイプ設定コマンド
    set_type_parser = subparsers.add_parser('set-type', help='バックアップの種類を設定する')
    set_type_parser.add_argument('type', choices=['full', 'differential'], help='バックアップの種類')
    
    # 圧縮設定コマンド
    set_compress_parser = subparsers.add_parser('set-compress', help='圧縮設定を行う')
    set_compress_parser.add_argument('--enable', action='store_true', help='圧縮を有効にする')
    set_compress_parser.add_argument('--disable', action='store_true', help='圧縮を無効にする')
    set_compress_parser.add_argument('--format', choices=['zip', 'tar.gz'], help='圧縮形式')
    
    # スケジュール設定コマンド
    set_schedule_parser = subparsers.add_parser('set-schedule', help='スケジュール設定を行う')
    set_schedule_parser.add_argument('type', choices=['daily', 'weekly', 'monthly'], help='スケジュールの種類')
    set_schedule_parser.add_argument('--time', help='実行時刻 (例: 00:00)')
    set_schedule_parser.add_argument('--day', type=int, choices=range(7), help='実行曜日 (0-6, 0=月曜日)')
    set_schedule_parser.add_argument('--full-day', type=int, choices=range(7), help='完全バックアップを行う曜日 (0-6, 0=月曜日)')
    
    # 並列処理設定コマンド
    set_workers_parser = subparsers.add_parser('set-workers', help='並列処理のワーカー数を設定する')
    set_workers_parser.add_argument('workers', type=int, help='ワーカー数')
    
    # 履歴表示コマンド
    list_parser = subparsers.add_parser('list', help='バックアップ履歴を表示する')

    # 設定表示コマンド
    show_config_parser = subparsers.add_parser('show-config', help='現在の設定を表示する')

    # カタログ検索コマンド
    catalog_search_parser = subparsers.add_parser(
        'catalog-search',
        help='カタログからファイルを検索する（例: "*.py" や "*report*"）',
    )
    catalog_search_parser.add_argument(
        'pattern',
        help='検索パターン。glob 形式（*.py）または SQL LIKE 形式（%%.py）が使えます（* は %% に変換）。',
    )

    # ファイル履歴表示コマンド
    catalog_history_parser = subparsers.add_parser(
        'catalog-history',
        help='特定ファイルのバックアップ履歴を表示する',
    )
    catalog_history_parser.add_argument('rel_path', help='カタログ内の相対パス（完全一致）')

    # ファイル単位の部分復元コマンド
    restore_file_parser = subparsers.add_parser(
        'restore-file',
        help='カタログを使って特定ファイルのみを復元する',
    )
    restore_file_parser.add_argument('rel_path', help='復元するファイルの相対パス')
    restore_file_parser.add_argument('destination', help='復元先ディレクトリ')
    restore_file_id_group = restore_file_parser.add_mutually_exclusive_group()
    restore_file_id_group.add_argument('--id', type=int, metavar='CATALOG_ID',
                                       help='バックアップ ID（catalog-search / list で確認）')
    restore_file_id_group.add_argument('--latest', action='store_true',
                                       help='最新バックアップからファイルを復元する')

    # SFTP バックアップ先設定コマンド
    set_sftp_parser = subparsers.add_parser(
        'set-sftp-destination',
        help='SFTP/SSH バックアップ先を設定する',
    )
    set_sftp_parser.add_argument('--host', required=True, help='SFTP サーバーのホスト名または IP')
    set_sftp_parser.add_argument('--user', required=True, help='SSH ログインユーザー名')
    set_sftp_parser.add_argument('--key-file', required=True, help='SSH 秘密鍵ファイルのパス')
    set_sftp_parser.add_argument('--remote-path', required=True,
                                 help='リモートサーバー上のバックアップ保存ディレクトリ')
    set_sftp_parser.add_argument('--port', type=int, default=22, help='SSH ポート番号（デフォルト: 22）')
    set_sftp_parser.add_argument('--max-generations', type=int, default=0,
                                 help='保持する世代数（0 = 無制限、デフォルト: 0）')

    # SFTP 接続テストコマンド
    subparsers.add_parser('test-connection', help='SFTP 接続をテストする')

    return parser


def main():
    """メイン関数"""
    parser = create_parser()
    args = parser.parse_args()
    
    # バックアップマネージャーのインスタンスを作成
    backup_mgr = BackupManager()
    
    if args.command == 'run':
        # バックアップを実行
        success = backup_mgr.run_backup()
        if success:
            # 最新の履歴エントリからサマリーを表示
            history = backup_mgr.list_backups()
            if history:
                last = history[-1]
                btype = "完全" if last.get("type") == "full" else "差分"
                size_mb = last.get("size", 0) / (1024 * 1024)
                elapsed = last.get("elapsed_time", 0)
                print("=" * 50)
                print("バックアップ完了")
                print(f"  種類       : {btype}バックアップ")
                print(f"  保存先     : {last.get('path', '')}")
                print(f"  ファイル数 : {last.get('file_count', 0)} 件")
                print(f"  サイズ     : {size_mb:.2f} MB")
                print(f"  処理時間   : {elapsed:.2f} 秒")
                if last.get("errors", 0) > 0:
                    print(f"  警告       : {last['errors']} 件のエラーがありました（backup.log を確認）")
                print("=" * 50)
            else:
                print("バックアップが正常に完了しました")
        else:
            print("バックアップに失敗しました。ログ（backup.log）を確認してください")

    elif args.command == 'restore':
        # バックアップを復元
        # 復元元の決定
        backup_path = None
        if args.latest:
            latest_id = backup_mgr.get_latest_backup_id()
            if latest_id is None:
                print("カタログにバックアップが登録されていません。先に run を実行してください")
                return
            backups = backup_mgr.catalog.list_backups()
            target = next((b for b in backups if b["id"] == latest_id), None)
            if target is None:
                print("最新バックアップの情報が見つかりません")
                return
            backup_path = target["backup_path"]
            print(f"最新バックアップを使用: [{latest_id}] {target['timestamp']}  {backup_path}")
        elif args.id is not None:
            backups = backup_mgr.catalog.list_backups()
            target = next((b for b in backups if b["id"] == args.id), None)
            if target is None:
                print(f"カタログ ID {args.id} のバックアップが見つかりません（list コマンドで ID を確認してください）")
                return
            backup_path = target["backup_path"]
            print(f"バックアップを使用: [{args.id}] {target['timestamp']}  {backup_path}")
        elif args.path:
            backup_path = args.path
        else:
            print("復元元を指定してください: --path BACKUP_PATH | --id CATALOG_ID | --latest")
            return

        dry_run = args.dry_run
        if dry_run:
            print("※ ドライランモード: ファイルのコピーは行いません")
        success = backup_mgr.restore_backup(backup_path, args.destination, dry_run=dry_run)
        if success:
            if not dry_run:
                print(f"バックアップを復元しました: {args.destination}")
        else:
            print("バックアップの復元に失敗しました。ログ（backup.log）を確認してください")

    elif args.command == 'add-source':
        # バックアップ元を追加
        backup_mgr.add_source(args.path)
        
    elif args.command == 'remove-source':
        # バックアップ元を削除
        backup_mgr.remove_source(args.path)
        
    elif args.command == 'set-destination':
        # バックアップ先を設定
        backup_mgr.set_destination(args.path)
        
    elif args.command == 'set-type':
        # バックアップの種類を設定
        backup_mgr.set_backup_type(args.type)
        
    elif args.command == 'set-compress':
        # 圧縮設定
        if args.enable and args.disable:
            print("--enable と --disable は同時に指定できません")
            return
            
        if args.enable:
            backup_mgr.set_compression(True, args.format)
        elif args.disable:
            backup_mgr.set_compression(False)
        elif args.format:
            backup_mgr.set_compression(backup_mgr.config["compress"], args.format)
            
    elif args.command == 'set-schedule':
        # スケジュール設定
        backup_mgr.set_schedule(args.type, args.time, args.day, args.full_day)
        
    elif args.command == 'set-workers':
        # 並列処理ワーカー数設定
        backup_mgr.set_max_workers(args.workers)
        
    elif args.command == 'list':
        # バックアップ履歴を表示（catalog DB と config 履歴を統合して表示）
        catalog_backups = backup_mgr.catalog.list_backups()
        config_history = backup_mgr.list_backups()

        if not catalog_backups and not config_history:
            print("バックアップ履歴はありません")
        else:
            # catalog_backups を優先。catalog に未登録の旧形式履歴も補完表示
            if catalog_backups:
                print(f"合計 {len(catalog_backups)} 件のバックアップ（catalog）:")
                print(f"{'ID':>4}  {'日時':<17}  {'種類':^4}  {'ファイル数':>8}  {'サイズ':>8}  パス")
                print("-" * 80)
                for b in catalog_backups:
                    btype = "完全" if b["backup_type"] == "full" else "差分"
                    size_mb = b.get("total_size", 0) / (1024 * 1024)
                    print(f"{b['id']:>4}  {b['timestamp']:<17}  {btype:^4}  "
                          f"{b.get('file_count', 0):>8}  {size_mb:>7.2f}MB  {b['backup_path']}")
                print()
                print("復元例: python SyncVault.py restore --id <ID> <復元先>")
                print("        python SyncVault.py restore --latest <復元先>")
            else:
                # catalog が空の場合は旧形式の config 履歴を表示
                print(f"合計 {len(config_history)} 件のバックアップ（設定ファイル）:")
                for i, backup in enumerate(config_history, 1):
                    timestamp = backup["timestamp"]
                    backup_type = "完全" if backup["type"] == "full" else "差分"
                    path = backup["path"]
                    size_mb = backup["size"] / (1024 * 1024)
                    if "file_count" in backup:
                        elapsed = backup.get("elapsed_time", 0)
                        print(f"{i}. [{timestamp}] {backup_type}バックアップ - {path}")
                        print(f"   サイズ: {size_mb:.2f} MB, ファイル数: {backup['file_count']}")
                        print(f"   処理: {backup.get('processed', 0)}件, スキップ: {backup.get('skipped', 0)}件, エラー: {backup.get('errors', 0)}件")
                        print(f"   処理時間: {elapsed:.2f}秒")
                    else:
                        print(f"{i}. [{timestamp}] {backup_type}バックアップ - {path} ({size_mb:.2f} MB)")
                
    elif args.command == 'show-config':
        # 現在の設定を表示
        config = backup_mgr.config
        print("現在の設定:")
        # --- プラットフォーム情報 ---
        print(f"動作 OS  : {PlatformHelper.platform_name()}")
        print(f"設定ファイル: {backup_mgr.config_path}")
        print(f"ログファイル: {_LOG_PATH}")
        print(f"一時ディレクトリ: {tempfile.gettempdir()}")
        print()
        print(f"バックアップ元: {', '.join(config['sources'])}" if config['sources'] else "バックアップ元: 未設定")
        dest_type = config.get("destination_type", "local")
        if dest_type == "sftp":
            sftp = config.get("sftp", {})
            print(f"バックアップ先: sftp://{sftp.get('username', '')}@{sftp.get('host', '')}:{sftp.get('port', 22)}{sftp.get('remote_path', '')}")
            print(f"  認証鍵      : {sftp.get('key_file', '未設定')}")
            print(f"  known_hosts : {sftp.get('known_hosts_file', '~/.syncvault_known_hosts')}")
            gen = sftp.get("max_generations", 0)
            print(f"  世代管理    : {'無制限' if not gen else f'{gen} 世代'}")
        else:
            print(f"バックアップ先: {config['destination']}" if config['destination'] else "バックアップ先: 未設定")
        print(f"バックアップの種類: {'完全' if config['backup_type'] == 'full' else '差分'}")
        print(f"圧縮: {'有効' if config['compress'] else '無効'}")
        if config['compress']:
            print(f"圧縮形式: {config['compression_format']}")

        print(f"並列処理ワーカー数: {config.get('max_workers', DEFAULT_MAX_WORKERS)}")

        if config.get('exclude_patterns'):
            print(f"除外パターン: {', '.join(config['exclude_patterns'])}")

        schedule = config['schedule']
        schedule_types = {"daily": "毎日", "weekly": "毎週", "monthly": "毎月"}
        print(f"スケジュール: {schedule_types.get(schedule['type'], schedule['type'])}")
        print(f"実行時刻: {schedule['time']}")

        if schedule['type'] in ["weekly", "monthly"]:
            weekdays = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"]
            day = schedule['day_of_week']
            print(f"実行曜日: {weekdays[day]}")

        full_day = schedule['full_backup_day']
        weekdays = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"]
        print(f"完全バックアップ実行曜日: {weekdays[full_day]}")
    
    elif args.command == 'catalog-search':
        # カタログからファイルを検索
        # glob パターン（*）を SQL LIKE パターン（%）に自動変換
        raw_pattern = args.pattern
        if "*" in raw_pattern or "?" in raw_pattern:
            sql_pattern = raw_pattern.replace("%", r"\%").replace("*", "%").replace("?", "_")
            print(f"ヒント: glob パターン '{raw_pattern}' を SQL パターン '{sql_pattern}' に変換しました")
        else:
            sql_pattern = raw_pattern
        results = backup_mgr.catalog.find_file_backups(sql_pattern)
        if not results:
            print(f"'{raw_pattern}' に一致するファイルが見つかりませんでした")
            print("ヒント: ワイルドカードには * (例: *.py) または % (例: %.py) が使えます")
        else:
            print(f"{len(results)} 件のファイルが見つかりました:")
            print(f"{'ID':>4}  {'日時':<17}  {'種類':^4}  {'サイズ':>8}  ファイルパス")
            print("-" * 70)
            for r in results:
                size_kb = r["file_size"] / 1024
                btype = "完全" if r["backup_type"] == "full" else "差分"
                print(
                    f"  [{r['backup_id']:>3}] {r['timestamp']}  ({btype})  "
                    f"{size_kb:>7.1f}KB  {r['rel_path']}"
                )
            print()
            print("特定ファイルを復元: python SyncVault.py restore-file <ファイルパス> <復元先> --id <ID>")

    elif args.command == 'catalog-history':
        # 特定ファイルの履歴を表示
        history = backup_mgr.catalog.get_file_history(args.rel_path)
        if not history:
            print(f"ファイルのバックアップ履歴が見つかりません: {args.rel_path}")
        else:
            print(f"'{args.rel_path}' のバックアップ履歴 ({len(history)} 件):")
            for r in history:
                size_kb = r["file_size"] / 1024
                btype = "完全" if r["backup_type"] == "full" else "差分"
                print(
                    f"  ID={r['backup_id']}  [{r['timestamp']}]  {btype}  "
                    f"{size_kb:.1f} KB  hash:{r['file_hash'][:8]}..."
                )

    elif args.command == 'restore-file':
        # ファイル単位の部分復元
        if args.latest:
            backup_id = backup_mgr.get_latest_backup_id()
            if backup_id is None:
                print("カタログにバックアップが登録されていません。先に run を実行してください")
                return
            print(f"最新バックアップ（ID={backup_id}）からファイルを復元します")
        elif args.id is not None:
            backup_id = args.id
        else:
            print("バックアップを指定してください: --id CATALOG_ID | --latest")
            print("ヒント: 利用可能な ID は 'python SyncVault.py list' または 'catalog-search' で確認できます")
            return
        success = backup_mgr.restore_file(args.rel_path, backup_id, args.destination)
        if success:
            print(f"ファイルを復元しました: {args.destination}")
        else:
            print("ファイルの復元に失敗しました。ログ（backup.log）を確認してください")

    elif args.command == 'set-sftp-destination':
        # SFTP バックアップ先を設定
        backup_mgr.set_sftp_destination(
            host=args.host,
            username=args.user,
            key_file=args.key_file,
            remote_path=args.remote_path,
            port=args.port,
            max_generations=args.max_generations,
        )
        sftp = backup_mgr.config.get("sftp", {})
        print(f"SFTP バックアップ先を設定しました:")
        print(f"  ホスト    : {sftp['host']}:{sftp.get('port', 22)}")
        print(f"  ユーザー  : {sftp['username']}")
        print(f"  鍵ファイル: {sftp['key_file']}")
        print(f"  リモートパス: {sftp['remote_path']}")
        gen = sftp.get('max_generations', 0)
        print(f"  世代管理  : {'無制限' if not gen else f'{gen} 世代'}")
        print("接続テスト: python SyncVault.py test-connection")

    elif args.command == 'test-connection':
        # SFTP 接続テスト
        print("SFTP 接続をテストしています...")
        success = backup_mgr.test_sftp_connection()
        if success:
            print("接続テスト成功")
        else:
            print("接続テストに失敗しました。ログ（backup.log）を確認してください")

    else:
        # コマンドが指定されていない場合はヘルプを表示
        parser.print_help()


if __name__ == "__main__":
    main()