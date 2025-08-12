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
import hashlib
import platform
import time
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

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backup.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("backup")

# 設定ファイルのデフォルトパス
DEFAULT_CONFIG_PATH = os.path.expanduser("~/.backup_config.json")

# 並列処理の設定
DEFAULT_MAX_WORKERS = min(32, (os.cpu_count() or 1) * 2)
CHUNK_SIZE = 1024 * 1024  # 1MB chunks for file reading

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
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        """初期化処理
        
        Args:
            config_path: 設定ファイルのパス
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.max_workers = self.config.get("max_workers", DEFAULT_MAX_WORKERS)
        
    def _load_config(self) -> Dict[str, Any]:
        """設定ファイルを読み込む
        
        Returns:
            設定データを含む辞書
        """
        if not os.path.exists(self.config_path):
            # デフォルト設定
            default_config = {
                "sources": [],
                "destination": "",
                "backup_type": "full",  # full または differential
                "compress": True,
                "compression_format": "zip",  # zip または tar.gz
                "schedule": {
                    "type": "daily",  # daily, weekly, monthly
                    "time": "00:00",
                    "day_of_week": 0,  # 0 = 月曜日
                    "full_backup_day": 0  # 0 = 月曜日
                },
                "history": [],
                "max_workers": DEFAULT_MAX_WORKERS,  # 並列処理の最大ワーカー数
                "exclude_patterns": [  # 除外パターン
                    "*.tmp",
                    "*.temp",
                    "~*",
                    "Thumbs.db",
                    ".DS_Store"
                ]
            }
            
            # デフォルト設定を保存
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4)
                
            return default_config
        
        # 既存の設定ファイルを読み込む
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 新しい設定項目がない場合はデフォルト値を追加
                if "max_workers" not in config:
                    config["max_workers"] = DEFAULT_MAX_WORKERS
                if "exclude_patterns" not in config:
                    config["exclude_patterns"] = ["*.tmp", "*.temp", "~*", "Thumbs.db", ".DS_Store"]
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
            if pattern.startswith("*") and filename.endswith(pattern[1:]):
                return True
            elif pattern.endswith("*") and filename.startswith(pattern[:-1]):
                return True
            elif pattern == filename:
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
                     file_processor: FileProcessor) -> Optional[Tuple[str, str]]:
        """単一ファイルを処理する
        
        Args:
            file_info: (絶対パス, 相対パス)のタプル
            temp_dir: 一時ディレクトリ
            last_manifest: 前回のマニフェスト
            is_full_backup: 完全バックアップかどうか
            file_processor: FileProcessorインスタンス
            
        Returns:
            成功時は(相対パス, ハッシュ値)のタプル、スキップ/エラー時はNone
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
            
            return (rel_path, file_hash)
            
        except Exception as e:
            logger.error(f"ファイル処理エラー: {file_path} - {str(e)}")
            file_processor.update_stats(error=1)
            return None
    
    def _get_last_backup_manifest(self) -> Dict[str, str]:
        """最後のバックアップのマニフェストを取得する
        
        Returns:
            ファイルパスとハッシュ値の辞書
        """
        if not self.config["history"]:
            return {}
            
        last_backup = self.config["history"][-1]
        manifest_path = last_backup.get("manifest_path")
        
        if not manifest_path or not os.path.exists(manifest_path):
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
    
    def run_backup(self) -> bool:
        """バックアップを実行する（並列処理版）
        
        Returns:
            バックアップが成功した場合はTrue
        """
        if not self.config["sources"]:
            logger.error("バックアップ元が設定されていません")
            return False
            
        if not self.config["destination"]:
            logger.error("バックアップ先が設定されていません")
            return False
            
        # バックアップの種類を判断
        is_full_backup = self.should_run_full_backup()
        backup_type = "完全" if is_full_backup else "差分"
        logger.info(f"{backup_type}バックアップを開始します")
        logger.info(f"並列処理ワーカー数: {self.max_workers}")
        
        # 一時ディレクトリを作成
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_dir = os.path.join(os.path.expanduser("~"), f".backup_temp_{timestamp}")
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
                            rel_path, file_hash = result
                            with manifest_lock:
                                manifest[rel_path] = file_hash
                                
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
            
            # マニフェストファイルを作成
            manifest_path = os.path.join(temp_dir, "manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4)
                
            # バックアップ先のファイル名
            backup_filename = self._create_backup_filename(is_full_backup)
            backup_path = os.path.join(self.config["destination"], backup_filename)
            
            # 圧縮するかどうかで処理を分岐
            if self.config["compress"]:
                logger.info("バックアップを圧縮中...")
                self._compress_directory(temp_dir, backup_path)
                logger.info(f"バックアップを圧縮しました: {backup_path}")
            else:
                # 圧縮なしの場合はディレクトリごとコピー
                shutil.copytree(temp_dir, backup_path)
                logger.info(f"バックアップを作成しました: {backup_path}")
            
            # 履歴に追加
            backup_record = {
                "timestamp": timestamp,
                "type": "full" if is_full_backup else "differential",
                "path": backup_path,
                "manifest_path": manifest_path if not self.config["compress"] else None,
                "size": os.path.getsize(backup_path) if os.path.exists(backup_path) else 0,
                "file_count": len(manifest),
                "processed": stats["processed"],
                "skipped": stats["skipped"],
                "errors": stats["errors"],
                "elapsed_time": elapsed_time
            }
            
            self.config["history"].append(backup_record)
            self._save_config()
            
            return True
            
        except Exception as e:
            logger.error(f"バックアップ中にエラーが発生しました: {str(e)}")
            return False
            
        finally:
            # 一時ディレクトリを削除
            if os.path.exists(temp_dir) and self.config["compress"]:
                shutil.rmtree(temp_dir)

    def restore_backup(self, backup_path: str, restore_path: str) -> bool:
        """バックアップを復元する

        Args:
            backup_path: 復元するバックアップファイルまたはディレクトリのパス
            restore_path: 復元先ディレクトリのパス

        Returns:
            復元が成功した場合は True
        """
        if not os.path.exists(backup_path):
            logger.error(f"バックアップファイルが見つかりません: {backup_path}")
            return False

        try:
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
    restore_parser.add_argument('source', help='復元するバックアップのパス')
    restore_parser.add_argument('destination', help='復元先のパス')
    
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
            print("バックアップが正常に完了しました")
        else:
            print("バックアップに失敗しました。ログを確認してください")

    elif args.command == 'restore':
        # バックアップを復元
        success = backup_mgr.restore_backup(args.source, args.destination)
        if success:
            print("バックアップを復元しました")
        else:
            print("バックアップの復元に失敗しました。ログを確認してください")

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
        # バックアップ履歴を表示
        backups = backup_mgr.list_backups()
        if not backups:
            print("バックアップ履歴はありません")
        else:
            print(f"合計 {len(backups)} 件のバックアップ:")
            for i, backup in enumerate(backups, 1):
                timestamp = backup["timestamp"]
                backup_type = "完全" if backup["type"] == "full" else "差分"
                path = backup["path"]
                size_mb = backup["size"] / (1024 * 1024)
                
                # 新しい統計情報を表示（存在する場合）
                if "file_count" in backup:
                    elapsed = backup.get("elapsed_time", 0)
                    print(f"{i}. [{timestamp}] {backup_type}バックアップ - {path}")
                    print(f"   サイズ: {size_mb:.2f} MB, ファイル数: {backup['file_count']}")
                    print(f"   処理: {backup.get('processed', 0)}件, スキップ: {backup.get('skipped', 0)}件, エラー: {backup.get('errors', 0)}件")
                    print(f"   処理時間: {elapsed:.2f}秒")
                else:
                    # 旧形式の履歴
                    print(f"{i}. [{timestamp}] {backup_type}バックアップ - {path} ({size_mb:.2f} MB)")
                
    elif args.command == 'show-config':
        # 現在の設定を表示
        config = backup_mgr.config
        print("現在の設定:")
        print(f"バックアップ元: {', '.join(config['sources'])}" if config['sources'] else "バックアップ元: 未設定")
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
    
    else:
        # コマンドが指定されていない場合はヘルプを表示
        parser.print_help()


if __name__ == "__main__":
    main()