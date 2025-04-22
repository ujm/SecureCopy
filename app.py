#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
クロスプラットフォームバックアッププログラム
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

class BackupManager:
    """バックアップ処理を管理するクラス"""
    
    def __init__(self, config_path: str = DEFAULT_CONFIG_PATH):
        """初期化処理
        
        Args:
            config_path: 設定ファイルのパス
        """
        self.config_path = config_path
        self.config = self._load_config()
        
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
                "history": []
            }
            
            # デフォルト設定を保存
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=4)
                
            return default_config
        
        # 既存の設定ファイルを読み込む
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
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
        """ファイルのハッシュ値を計算する
        
        Args:
            filepath: ファイルパス
            
        Returns:
            ファイルのMD5ハッシュ値
        """
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
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
        """バックアップを実行する
        
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
        
        # 一時ディレクトリを作成
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_dir = os.path.join(os.path.expanduser("~"), f".backup_temp_{timestamp}")
        os.makedirs(temp_dir, exist_ok=True)
        
        try:
            # マニフェスト（ファイルとハッシュ値のマッピング）
            manifest = {}
            
            # 前回のバックアップマニフェスト
            last_manifest = self._get_last_backup_manifest() if not is_full_backup else {}
            
            # バックアップ元をコピー
            for source_path in self.config["sources"]:
                if not os.path.exists(source_path):
                    logger.warning(f"バックアップ元が存在しません: {source_path}")
                    continue
                    
                # ソースパスがファイルかディレクトリか判断
                if os.path.isfile(source_path):
                    # ファイルの場合
                    file_hash = self._get_file_hash(source_path)
                    rel_path = os.path.basename(source_path)
                    
                    # 差分バックアップの場合はチェック
                    if not is_full_backup and rel_path in last_manifest and last_manifest[rel_path] == file_hash:
                        # 変更なしの場合はスキップ
                        logger.debug(f"変更なしのためスキップします: {source_path}")
                        continue
                        
                    # ファイルをコピー
                    dest_file = os.path.join(temp_dir, rel_path)
                    shutil.copy2(source_path, dest_file)
                    manifest[rel_path] = file_hash
                    
                else:
                    # ディレクトリの場合
                    for root, _, files in os.walk(source_path):
                        for file in files:
                            file_path = os.path.join(root, file)
                            # ソースからの相対パス
                            rel_path = os.path.relpath(file_path, os.path.dirname(source_path))
                            
                            file_hash = self._get_file_hash(file_path)
                            
                            # 差分バックアップの場合はチェック
                            if not is_full_backup and rel_path in last_manifest and last_manifest[rel_path] == file_hash:
                                # 変更なしの場合はスキップ
                                logger.debug(f"変更なしのためスキップします: {file_path}")
                                continue
                                
                            # 出力先のディレクトリを作成
                            dest_dir = os.path.join(temp_dir, os.path.dirname(rel_path))
                            os.makedirs(dest_dir, exist_ok=True)
                            
                            # ファイルをコピー
                            dest_file = os.path.join(temp_dir, rel_path)
                            shutil.copy2(file_path, dest_file)
                            manifest[rel_path] = file_hash
            
            # マニフェストファイルを作成
            manifest_path = os.path.join(temp_dir, "manifest.json")
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=4)
                
            # バックアップ先のファイル名
            backup_filename = self._create_backup_filename(is_full_backup)
            backup_path = os.path.join(self.config["destination"], backup_filename)
            
            # 圧縮するかどうかで処理を分岐
            if self.config["compress"]:
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
                "size": os.path.getsize(backup_path) if os.path.exists(backup_path) else 0
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
    parser = argparse.ArgumentParser(description='クロスプラットフォームバックアッププログラム')
    
    subparsers = parser.add_subparsers(dest='command', help='コマンド')
    
    # バックアップ実行コマンド
    run_parser = subparsers.add_parser('run', help='バックアップを実行する')
    
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
