#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
資料夾批量文本與圖像PDF生成系統
============================
自動掃描指定資料夾，將其中的TXT文件與PNG圖像按照配對規則合併為PDF文檔。

系統特性:
1. 自動遞歸掃描目錄結構
2. 智能文件配對算法
3. 多線程批量處理
4. 可配置輸出規則
5. 詳細日誌記錄
"""

import os
import sys
import re
import time
import logging
import argparse
import threading
import queue
from pathlib import Path
from typing import Dict, List, Tuple, Set, Optional, Union, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import itertools
import json
import shutil
import tempfile
import multiprocessing
from datetime import datetime

# 配置日誌系統
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("PDF-BatchGenerator")

# 第三方庫依賴
try:
    from reportlab.lib.pagesizes import A4, letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm, mm, inch
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, 
        Table, TableStyle, Frame, BaseDocTemplate, PageTemplate
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    
    from PIL import Image as PILImage
    
    # 可選增強功能依賴庫
    try:
        import chardet  # 文件編碼檢測
        CHARDET_AVAILABLE = True
    except ImportError:
        CHARDET_AVAILABLE = False
        
except ImportError as e:
    print(f"錯誤: 缺少必要依賴庫。請安裝需要的庫: {e}")
    print("請執行: pip install reportlab pillow chardet")
    sys.exit(1)

# 核心常量定義
DEFAULT_OUTPUT_DIR = "output_pdfs"
TEXT_EXTENSIONS = {'.txt'}
IMAGE_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.gif', '.bmp'}

# 文件匹配模式
FILE_MATCH_PATTERNS = [
    r"^([a-f0-9]+)_.*$",           # 匹配ID_任意內容模式 (針對CSDN文章)
    r"^(.+?)[\._](\d+)\.txt$",     # 匹配名稱_編號.txt 或 名稱.編號.txt
    r"^(.+)\.txt$"                 # 匹配名稱.txt (無編號)
]

IMAGE_MATCH_PATTERNS = [
    r"^img_([a-f0-9]+)_[a-f0-9]+$",  # 匹配新的圖片命名模式 img_ID_hash.png
    r"^(.+?)[\._](\d+)\.png$",     # 匹配名稱_編號.png 或 名稱.編號.png
    r"^(.+)\.png$"                 # 匹配名稱.png (無編號)
]

# 自定義異常類別
class BatchProcessingError(Exception):
    """批量處理基礎異常類別"""
    pass


class FileProcessingError(BatchProcessingError):
    """文件處理錯誤"""
    pass


class PDFGenerationError(BatchProcessingError):
    """PDF生成錯誤"""
    pass


# 數據結構定義
@dataclass
class FileMetadata:
    """文件元數據結構"""
    path: Path                     # 文件路徑
    basename: str                  # 基礎文件名(無擴展名)
    extension: str                 # 文件擴展名
    match_key: str = None          # 匹配鍵(用於配對文件)
    match_index: int = 0           # 匹配索引(順序號)
    size: int = 0                  # 文件大小(位元組)
    last_modified: float = 0       # 最後修改時間
    
    def __post_init__(self):
        """初始化後處理，填充額外屬性"""
        if not self.size:
            try:
                self.size = os.path.getsize(self.path)
            except (OSError, IOError):
                self.size = 0
                
        if not self.last_modified:
            try:
                self.last_modified = os.path.getmtime(self.path)
            except (OSError, IOError):
                self.last_modified = 0


@dataclass
class FileGroup:
    """文件分組結構，關聯相同主題的文本和圖像"""
    group_key: str                           # 分組鍵
    text_files: List[FileMetadata] = None    # 文本文件列表
    image_files: List[FileMetadata] = None   # 圖像文件列表
    output_filename: str = None              # 輸出PDF文件名
    
    def __post_init__(self):
        """初始化後處理，確保列表存在"""
        if self.text_files is None:
            self.text_files = []
        if self.image_files is None:
            self.image_files = []
        
        # 設置默認輸出文件名
        if not self.output_filename and self.group_key:
            self.output_filename = f"{self.group_key}.pdf"


# 1. 文件掃描與分類引擎
class FileScanner:
    """資料夾掃描與文件分類系統"""
    
    def __init__(self, root_dir: Union[str, Path], recursive: bool = True):
        """
        初始化文件掃描器
        
        Args:
            root_dir: 掃描的根目錄
            recursive: 是否遞歸掃描子目錄
        """
        self.root_dir = Path(root_dir)
        self.recursive = recursive
        
        # 驗證目錄存在
        if not self.root_dir.exists() or not self.root_dir.is_dir():
            raise ValueError(f"指定的目錄不存在或不是有效目錄: {self.root_dir}")
        
        # 文件分類存儲
        self.text_files: List[FileMetadata] = []
        self.image_files: List[FileMetadata] = []
        
        # 掃描狀態
        self.scan_complete = False
        self.total_files_scanned = 0
    
    def scan(self) -> Tuple[List[FileMetadata], List[FileMetadata]]:
        """
        執行目錄掃描，分類文件
        
        Returns:
            Tuple[List[FileMetadata], List[FileMetadata]]: 文本文件和圖像文件列表
        """
        logger.info(f"開始掃描目錄: {self.root_dir}")
        self.text_files = []
        self.image_files = []
        self.total_files_scanned = 0
        
        # 掃描目錄
        pattern = "**/*" if self.recursive else "*"
        
        for file_path in self.root_dir.glob(pattern):
            if file_path.is_file():
                self.total_files_scanned += 1
                ext = file_path.suffix.lower()
                
                if ext in TEXT_EXTENSIONS:
                    metadata = self._process_text_file(file_path)
                    if metadata:
                        self.text_files.append(metadata)
                        
                elif ext in IMAGE_EXTENSIONS:
                    metadata = self._process_image_file(file_path)
                    if metadata:
                        self.image_files.append(metadata)
        
        self.scan_complete = True
        logger.info(f"掃描完成, 共發現 {len(self.text_files)} 個文本文件和 {len(self.image_files)} 個圖像文件")
        
        return self.text_files, self.image_files
    
    def _process_text_file(self, file_path: Path) -> Optional[FileMetadata]:
        """處理文本文件，提取元數據和匹配關鍵信息"""
        try:
            basename = file_path.stem
            match_key, match_index = self._extract_match_info(basename, FILE_MATCH_PATTERNS)
            
            metadata = FileMetadata(
                path=file_path,
                basename=basename,
                extension=file_path.suffix.lower(),
                match_key=match_key,
                match_index=match_index
            )
            
            return metadata
        except Exception as e:
            logger.warning(f"處理文本文件時出錯 {file_path}: {e}")
            return None
    
    def _process_image_file(self, file_path: Path) -> Optional[FileMetadata]:
        """處理圖像文件，提取元數據和匹配關鍵信息"""
        try:
            basename = file_path.stem
            match_key, match_index = self._extract_match_info(basename, IMAGE_MATCH_PATTERNS)
            
            metadata = FileMetadata(
                path=file_path,
                basename=basename,
                extension=file_path.suffix.lower(),
                match_key=match_key,
                match_index=match_index
            )
            
            return metadata
        except Exception as e:
            logger.warning(f"處理圖像文件時出錯 {file_path}: {e}")
            return None
    
    def _extract_match_info(self, basename: str, patterns: List[str]) -> Tuple[str, int]:
        """
        從文件名中提取匹配信息
        
        Args:
            basename: 不含擴展名的文件名
            patterns: 匹配模式列表
            
        Returns:
            Tuple[str, int]: 匹配鍵和索引號
        """
        logger.debug(f"嘗試匹配文件名: {basename}")
        
        # 首先嘗試匹配帶有img_前綴的圖片文件 (這是最優先的匹配模式)
        img_prefix_match = re.match(r'^img_([a-f0-9]+)_[a-f0-9]+$', basename)
        if img_prefix_match:
            # 提取文章ID作為匹配鍵
            match_key = img_prefix_match.group(1)
            logger.debug(f"✓ 匹配到img_前綴圖片: {basename}, 匹配鍵={match_key}")
            return match_key, 0
        
        # 嘗試所有匹配模式
        for pattern in patterns:
            logger.debug(f"嘗試匹配模式: {pattern}")
            match = re.match(pattern, basename)
            if match:
                if len(match.groups()) > 1:
                    # 有編號的情況
                    match_key = match.group(1)
                    try:
                        match_index = int(match.group(2))
                    except (ValueError, IndexError):
                        match_index = 0
                else:
                    # 無編號的情況
                    match_key = match.group(1)
                    match_index = 0
                
                logger.debug(f"✓ 匹配到模式 {pattern}: {basename}, 匹配鍵={match_key}, 索引={match_index}")
                return match_key, match_index
        
        # 若無匹配，使用完整basename作為匹配鍵
        logger.debug(f"❌ 未找到匹配模式，使用完整basename: {basename}")
        return basename, 0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        獲取掃描統計信息
        
        Returns:
            Dict: 包含掃描統計數據的字典
        """
        if not self.scan_complete:
            return {"status": "incomplete"}
        
        # 計算各類型文件數量
        text_exts = {}
        for file in self.text_files:
            ext = file.extension
            text_exts[ext] = text_exts.get(ext, 0) + 1
        
        image_exts = {}
        for file in self.image_files:
            ext = file.extension
            image_exts[ext] = image_exts.get(ext, 0) + 1
        
        # 獲取匹配鍵統計
        text_keys = set(f.match_key for f in self.text_files if f.match_key)
        image_keys = set(f.match_key for f in self.image_files if f.match_key)
        common_keys = text_keys.intersection(image_keys)
        
        return {
            "status": "complete",
            "total_files": self.total_files_scanned,
            "text_files": len(self.text_files),
            "image_files": len(self.image_files),
            "text_extensions": text_exts,
            "image_extensions": image_exts,
            "text_keys": len(text_keys),
            "image_keys": len(image_keys),
            "common_keys": len(common_keys),
            "keys": list(common_keys)
        }


# 2. 文件分組與配對系統
class FileMatcher:
    """文件配對與分組系統"""
    
    def __init__(self, text_files: List[FileMetadata], image_files: List[FileMetadata]):
        """
        初始化文件配對器
        
        Args:
            text_files: 文本文件元數據列表
            image_files: 圖像文件元數據列表
        """
        self.text_files = text_files
        self.image_files = image_files
        self.groups: List[FileGroup] = []
    
    def match_files(self) -> List[FileGroup]:
        """
        執行文件配對邏輯，將文件組織成相關組
        
        Returns:
            List[FileGroup]: 文件分組列表
        """
        # 1. 按匹配鍵對文件進行分組
        text_by_key = self._group_by_key(self.text_files)
        image_by_key = self._group_by_key(self.image_files)
        
        # 調試輸出：檢查匹配鍵
        logger.debug(f"文本文件匹配鍵: {list(text_by_key.keys())}")
        logger.debug(f"圖像文件匹配鍵: {list(image_by_key.keys())}")
        
        # 2. 找出共同的匹配鍵
        all_keys = set(text_by_key.keys()).union(set(image_by_key.keys()))
        
        # 3. 為每個鍵創建文件組
        self.groups = []
        
        for key in all_keys:
            text_group = text_by_key.get(key, [])
            image_group = image_by_key.get(key, [])
            
            # 詳細調試輸出：檢查每個組的文件
            if text_group:
                logger.debug(f"匹配鍵 '{key}' 的文本文件: {[f.basename for f in text_group]}")
            if image_group:
                logger.debug(f"匹配鍵 '{key}' 的圖像文件: {[f.basename for f in image_group]}")
                # 檢查圖像文件路徑是否存在
                for img in image_group:
                    if not os.path.exists(img.path):
                        logger.warning(f"圖像文件不存在: {img.path}")
                    else:
                        logger.debug(f"圖像文件存在: {img.path} (大小: {os.path.getsize(img.path)} 字節)")
            
            # 只有當至少有一個文本文件時才創建組
            if text_group:
                # 按索引排序
                text_group.sort(key=lambda x: x.match_index)
                if image_group:
                    image_group.sort(key=lambda x: x.match_index)
                    logger.info(f"為匹配鍵 '{key}' 找到 {len(text_group)} 個文本文件和 {len(image_group)} 個圖像文件")
                else:
                    logger.warning(f"匹配鍵 '{key}' 只有文本文件，沒有匹配的圖像")
                
                group = FileGroup(
                    group_key=key,
                    text_files=text_group,
                    image_files=image_group,
                    output_filename=f"{key}.pdf"
                )
                self.groups.append(group)
        
        logger.info(f"文件配對完成，共生成 {len(self.groups)} 個文件組")
        return self.groups
    
    def _group_by_key(self, files: List[FileMetadata]) -> Dict[str, List[FileMetadata]]:
        """
        按匹配鍵對文件分組
        
        Args:
            files: 文件元數據列表
            
        Returns:
            Dict: 按匹配鍵分組的文件字典
        """
        result = {}
        for file in files:
            if file.match_key:
                if file.match_key not in result:
                    result[file.match_key] = []
                result[file.match_key].append(file)
        return result
    
    def get_matching_stats(self) -> Dict[str, Any]:
        """
        獲取配對統計信息
        
        Returns:
            Dict: 包含配對統計數據的字典
        """
        if not self.groups:
            return {"status": "no_matches"}
        
        # 計算統計數據
        total_texts = sum(len(g.text_files) for g in self.groups)
        total_images = sum(len(g.image_files) for g in self.groups)
        
        groups_with_images = sum(1 for g in self.groups if g.image_files)
        groups_text_only = sum(1 for g in self.groups if not g.image_files)
        
        return {
            "status": "matched",
            "total_groups": len(self.groups),
            "total_texts_matched": total_texts,
            "total_images_matched": total_images,
            "groups_with_images": groups_with_images,
            "groups_text_only": groups_text_only,
            "group_keys": [g.group_key for g in self.groups]
        }


# 3. PDF文檔生成引擎
class PDFGenerator:
    """PDF生成器類"""
    
    def __init__(self, output_file: str, article_id: str="未知", title: str="未知標題"):
        """
        初始化PDF生成器
        
        Args:
            output_file: 輸出的PDF文件路徑
            article_id: 文章的ID
            title: 文章的標題
        """
        self.output_file = output_file
        self.article_id = article_id
        self.title = title
        
        # 設置頁面大小和邊距
        self.width, self.height = A4
        self.left_margin = 2.5 * cm
        self.right_margin = 2.5 * cm
        self.top_margin = 2.0 * cm
        self.bottom_margin = 2.5 * cm
        
        # 先創建樣式表
        self.styles = getSampleStyleSheet()
        
        # 然後註冊字體
        self._register_fonts()
        
        # 設置文檔模板和樣式
        self.doc_template = self._setup_doc_template(self.output_file)

    def _register_fonts(self):
        """註冊字體"""
        # 方案1: 使用 reportlab 内置的字体支持
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))  # 华文宋体
        logger.info("成功註冊 STSong-Light CID 字體")
        
        chinese_font_name = 'STSong-Light'
        
        # 检查样式是否存在，避免重复定义
        if 'ChineseTitle_STSong' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseTitle_STSong',
                fontName=chinese_font_name,
                fontSize=18,
                leading=22,
                alignment=1,  # 居中
                spaceAfter=12
            ))
        if 'ChineseNormal_STSong' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseNormal_STSong',
                fontName=chinese_font_name,
                fontSize=10,
                leading=14
            ))
        if 'ChineseHeading_STSong' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseHeading_STSong',
                fontName=chinese_font_name,
                fontSize=14,
                leading=18,
                spaceAfter=6
            ))
        
        # 更新现有样式
        self.styles['Title'].fontName = chinese_font_name
        self.styles['Normal'].fontName = chinese_font_name
        self.styles['Heading1'].fontName = chinese_font_name
        self.styles['Heading2'].fontName = chinese_font_name
        self.styles['Heading3'].fontName = chinese_font_name
        
        # 方案2: 尝试使用 reportlab 的备用 CJK 支持
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont('HeiseiMin-W3'))  # 日文明朝体,但支持很多中文字符
        logger.info("成功註冊 HeiseiMin-W3 備用字體")
        
        chinese_font_name = 'HeiseiMin-W3'
        
        # 检查样式是否存在，避免重复定义
        if 'ChineseTitle_Heisei' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseTitle_Heisei',
                fontName=chinese_font_name,
                fontSize=18,
                leading=22,
                alignment=1,  # 居中
                spaceAfter=12
            ))
        if 'ChineseNormal_Heisei' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseNormal_Heisei',
                fontName=chinese_font_name,
                fontSize=10,
                leading=14
            ))
        if 'ChineseHeading_Heisei' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseHeading_Heisei',
                fontName=chinese_font_name,
                fontSize=14,
                leading=18,
                spaceAfter=6
            ))
        
        # 更新现有样式
        self.styles['Title'].fontName = chinese_font_name
        self.styles['Normal'].fontName = chinese_font_name
        self.styles['Heading1'].fontName = chinese_font_name
        self.styles['Heading2'].fontName = chinese_font_name
        self.styles['Heading3'].fontName = chinese_font_name
        
        self._setup_styles()
    
    def _setup_styles(self):
        """設置文檔樣式"""
        # 標題樣式
        if 'Title' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='Title',
                parent=self.styles['Heading1'],
                fontSize=16,
                spaceAfter=10,
                alignment=1  # 居中
            ))
        
        # 文件名樣式
        if 'Filename' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='Filename',
                parent=self.styles['Heading2'],
                fontSize=12,
                spaceAfter=6,
                spaceBefore=12
            ))
        
        # 正文樣式
        if 'BodyText' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='BodyText',
                parent=self.styles['Normal'],
                fontSize=10,
                leading=14,
                spaceBefore=6,
                spaceAfter=6
            ))
        
        # 圖片說明樣式
        if 'ImageCaption' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ImageCaption',
                parent=self.styles['Normal'],
                fontSize=9,
                leading=12,
                alignment=1,  # 居中
                spaceBefore=4,
                spaceAfter=12
            ))
    
    def generate_pdf(self, file_group: FileGroup) -> Optional[str]:
        """
        生成PDF文件
        
        Args:
            file_group: 文件組
            
        Returns:
            Optional[str]: 成功時返回生成的PDF文件路徑，失敗時返回None
        """
        try:
            # 檢查是否有文本文件
            if not file_group.text_files:
                logger.warning(f"組 {file_group.group_key} 沒有文本文件，無法生成PDF")
                return None
                
            # 檢查是否為付費未抓取內容
            first_text_file = file_group.text_files[0]
            if str(first_text_file.path).endswith("_付費內容_未抓取.txt") or "_付費內容_未抓取" in str(first_text_file.path):
                logger.warning(f"組 {file_group.group_key} 是付費未抓取內容，跳過PDF生成")
                return None
            
            # 提取標題 - 從第一個文本文件的basename中獲取
            if hasattr(first_text_file, 'title') and first_text_file.title:
                self.title = first_text_file.title
            else:
                self.title = first_text_file.basename.split('.')[0]
                
            # 檢查標題是否有效 - 檢查文本文件的前20行
            title_found = False
            article_content = ""
            
            try:
                with open(first_text_file.path, 'r', encoding='utf-8') as f:
                    first_lines = [line.strip() for line in f.readlines(2000)][:20]  # 修正: 正確讀取前20行
                    article_content = "\n".join(first_lines)
                    
                    # 檢查是否有看起來像有效標題的行
                    for line in first_lines:
                        if line and len(line) > 3 and len(line) < 100:
                            # 排除看起來像亂碼或URL的行
                            if not re.search(r'[^\w\s\u4e00-\u9fff.,?!:;\-_()[\]{}\'"`]', line) and not line.startswith('http'):
                                self.title = line
                                title_found = True
                                break
            except Exception as e:
                logger.warning(f"檢查標題時出現錯誤: {e}")
            
            # 如果沒有找到有效標題，記錄警告並跳過
            if not title_found:
                logger.warning(f"組 {file_group.group_key} 沒有有效標題，跳過PDF生成")
                logger.debug(f"文章內容開始: {article_content[:200]}...")
                return None
                
            logger.info(f"生成PDF: {self.output_file}, 標題: {self.title}")
            
            # 設置文檔模板
            self.doc_template = self._setup_doc_template(self.output_file)
            
            # 存储当前处理的文件组，以便在处理Markdown图片时使用
            self.current_group = file_group
            
            # 存储输入目录路径
            self.input_dir = os.path.dirname(os.path.dirname(self.output_file))
            if hasattr(first_text_file, 'path'):
                self.input_dir = os.path.dirname(first_text_file.path)
            
            # 創建文檔元素列表
            elements = []
            
            # 添加標題
            elements.append(Paragraph(self.title, self.styles['Title']))
            elements.append(Spacer(1, 0.25*inch))
            
            # 處理文本文件
            for text_file in file_group.text_files:
                self._add_text_content(elements, text_file)
                elements.append(Spacer(1, 0.15*inch))
            
            # 構建PDF
            try:
                self.doc_template.build(elements)
                logger.info(f"PDF生成成功: {self.output_file}")
                return self.output_file
            except Exception as e:
                logger.error(f"構建PDF時出錯: {e}")
                return None
                
        except Exception as e:
            logger.error(f"生成PDF時出錯: {e}")
            return None
    
    def _add_text_content(self, elements: List, text_file: FileMetadata):
        """添加文本內容到PDF文檔"""
        try:
            # 添加文件名標題
            if text_file.match_index > 0:
                filename_text = f"{text_file.basename} (#{text_file.match_index})"
            else:
                filename_text = text_file.basename
                
            elements.append(Paragraph(filename_text, self.styles['ChineseHeading1']))
            elements.append(Spacer(0, 0.2*inch))
            
            # 讀取文件內容
            encoding = self._detect_encoding(text_file.path)
            content = ""
            title = None
            
            with open(text_file.path, 'r', encoding=encoding) as f:
                lines = f.readlines()
                for line in lines[:20]:  # 检查前20行
                    line = line.strip()
                    if line.startswith('标题:') or line.startswith('標題:'):
                        title = line.split(':', 1)[1].strip()
                        break
                    elif line and len(line) > 3 and len(line) < 200:
                        # 排除看起来像URL或其他非标题内容的行
                        if not line.startswith(('http', 'www', '#', '/')):
                            title = line
                            break
                content = ''.join(lines)
            
            # 如果找到标题，更新输出文件名
            if title:
                # 清理标题中的非法字符
                safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)
                # 限制文件名长度
                if len(safe_title.encode('utf-8')) > 200:  # 限制字节长度
                    safe_title = safe_title[:50]  # 取前50个字符
                
                # 构建新的输出文件路径
                output_dir = os.path.dirname(self.output_file)
                new_filename = f"{safe_title}.pdf"
                new_output_path = os.path.join(output_dir, new_filename)
                
                # 更新输出文件路径
                self.output_file = new_output_path
                # 更新文档标题
                self.title = title
            
            # 清理文本
            content = self._clean_text(content)
            
            # 定义图片引用模式
            special_image_pattern = r'\[图片：(.*?)\]'
            markdown_image_pattern = r'!\[(.*?)\]\((.*?)\)'
            simple_image_pattern = r'!\[\]\((.*?)\)'
            
            # 跟踪已处理的图片
            processed_images = set()
            
            # 分段處理
            paragraphs = re.split(r'\n\s*\n', content)
            
            # 处理所有段落
            for i, para in enumerate(paragraphs):
                if not para.strip():
                    continue
                
                # 维护一个标记，表示该段落是否已经处理过图片
                paragraph_processed = False
                
                # 检查是否是特殊图片引用段落
                special_img_match = re.match(r'^\s*' + special_image_pattern + r'\s*$', para)
                if special_img_match and not paragraph_processed:
                    img_desc = special_img_match.group(1)
                    image_key = f"special:{img_desc}"
                    
                    if image_key not in processed_images:
                        self._add_special_image(elements, img_desc, image_map)
                        processed_images.add(image_key)
                        logger.debug(f"添加特殊图片: {img_desc}")
                    
                    paragraph_processed = True
                    continue
                
                # 检查是否是标准Markdown图片段落
                markdown_img_match = re.match(r'^\s*' + markdown_image_pattern + r'\s*$', para)
                if markdown_img_match and not paragraph_processed:
                    alt_text = markdown_img_match.group(1)
                    img_path = markdown_img_match.group(2)
                    image_key = f"markdown:{img_path}"
                    
                    if image_key not in processed_images:
                        self._add_markdown_image(elements, img_path, alt_text)
                        processed_images.add(image_key)
                        logger.debug(f"添加标准Markdown图片: {img_path}")
                    
                    paragraph_processed = True
                    continue
                
                # 检查是否是简化的Markdown图片段落
                simple_img_match = re.match(r'^\s*' + simple_image_pattern + r'\s*$', para)
                if simple_img_match and not paragraph_processed:
                    img_path = simple_img_match.group(1)
                    image_key = f"simple:{img_path}"
                    
                    if image_key not in processed_images:
                        self._add_markdown_image(elements, img_path, "")
                        processed_images.add(image_key)
                        logger.debug(f"添加简化Markdown图片: {img_path}")
                    
                    paragraph_processed = True
                    continue
                
                # 检查段落中是否包含特殊图片引用
                special_img_refs = re.findall(special_image_pattern, para)
                if special_img_refs and not paragraph_processed:
                    # 处理混合内容段落（文本和特殊图片引用）
                    text_parts = re.split(special_image_pattern, para)
                    
                    # 添加第一部分文本（如果有）
                    if text_parts[0].strip():
                        elements.append(Paragraph(text_parts[0].replace('\n', '<br/>'), 
                                               self.styles['ChineseNormal']))
                    
                    # 添加图片和后续文本
                    for j, img_desc in enumerate(special_img_refs):
                        image_key = f"special:{img_desc}"
                        if image_key not in processed_images:
                            self._add_special_image(elements, img_desc, image_map)
                            processed_images.add(image_key)
                            logger.debug(f"添加混合内容中的特殊图片: {img_desc}")
                        
                        # 添加图片后的文本（如果有）
                        if j+1 < len(text_parts) and text_parts[j+1].strip():
                            elements.append(Paragraph(text_parts[j+1].replace('\n', '<br/>'), 
                                                   self.styles['ChineseNormal']))
                    
                    paragraph_processed = True
                    continue
                
                # 检查段落中是否包含Markdown图片
                if not paragraph_processed:
                    # 处理标准Markdown图片
                    if re.search(markdown_image_pattern, para):
                        text_parts = re.split(markdown_image_pattern, para)
                        img_matches = re.findall(markdown_image_pattern, para)
                        
                        # 添加第一部分文本（如果有）
                        if text_parts[0].strip():
                            elements.append(Paragraph(text_parts[0].replace('\n', '<br/>'),
                                                  self.styles['ChineseNormal']))
                        
                        # 添加图片和后续文本
                        text_index = 1
                        for j, match in enumerate(img_matches):
                            alt_text, img_path = match
                            image_key = f"markdown:{img_path}"
                            
                            if image_key not in processed_images:
                                self._add_markdown_image(elements, img_path, alt_text)
                                processed_images.add(image_key)
                                logger.debug(f"添加混合内容中的标准Markdown图片: {img_path}")
                            
                            # 添加图片后的文本（如果有）
                            if text_index < len(text_parts) and text_parts[text_index].strip():
                                elements.append(Paragraph(text_parts[text_index].replace('\n', '<br/>'),
                                                       self.styles['ChineseNormal']))
                            text_index += 2
                    
                    # 处理简化Markdown图片
                    elif re.search(simple_image_pattern, para):
                        text_parts = re.split(simple_image_pattern, para)
                        img_paths = re.findall(simple_image_pattern, para)
                        
                        # 添加第一部分文本（如果有）
                        if text_parts[0].strip():
                            elements.append(Paragraph(text_parts[0].replace('\n', '<br/>'),
                                                  self.styles['ChineseNormal']))
                        
                        # 添加图片和后续文本
                        for j, img_path in enumerate(img_paths):
                            image_key = f"simple:{img_path}"
                            
                            if image_key not in processed_images:
                                self._add_markdown_image(elements, img_path, "")
                                processed_images.add(image_key)
                                logger.debug(f"添加混合内容中的简化Markdown图片: {img_path}")
                            
                            # 添加图片后的文本（如果有）
                            if j+1 < len(text_parts) and text_parts[j+1].strip():
                                elements.append(Paragraph(text_parts[j+1].replace('\n', '<br/>'),
                                                       self.styles['ChineseNormal']))
                    
                    # 如果段落中没有图片，直接添加文本
                    else:
                        elements.append(Paragraph(para.replace('\n', '<br/>'), 
                                               self.styles['ChineseNormal']))
                        elements.append(Spacer(0, 0.1*inch))
            
            # 在文本後添加更小的間距，以便與圖片更好地融合
            elements.append(Spacer(0, 0.15*inch))
            
        except Exception as e:
            logger.error(f"添加文本文件 {text_file.path} 內容時出錯: {e}")
            import traceback
            logger.error(f"錯誤詳情:\n{traceback.format_exc()}")
            # 添加錯誤信息
            elements.append(Paragraph(f"錯誤: 無法讀取文件 {text_file.basename}: {e}", self.styles['Normal']))
            elements.append(Spacer(0, 0.3*inch))
    
    def _add_special_image(self, elements: List, img_desc: str, image_map: Dict[str, str]):
        """处理特殊图片引用并添加图片"""
        try:
            # 尝试获取匹配的图片文件名
            if img_desc in image_map:
                img_filename = image_map[img_desc]
                logger.debug(f"特殊图片引用 '{img_desc}' 匹配到图片: {img_filename}")
                
                # 构建图片路径
                img_path = f"img/{img_filename}"
                self._add_markdown_image(elements, img_path, img_desc)
            else:
                # 查找文件组中是否有匹配的图片
                img_path = None
                if hasattr(self, 'current_group') and self.current_group and self.current_group.image_files:
                    # 尝试使用图片描述关键词匹配
                    keywords = re.findall(r'[a-zA-Z0-9]+', img_desc)
                    for img_file in self.current_group.image_files:
                        file_path = str(img_file.path)
                        # 检查文件名是否包含描述中的关键词
                        if any(keyword.lower() in file_path.lower() for keyword in keywords):
                            img_path = file_path
                            logger.debug(f"找到匹配关键词的图片: {img_path}")
                            break
                
                if img_path:
                    self._add_markdown_image(elements, img_path, img_desc)
                else:
                    logger.warning(f"未找到匹配的图片: {img_desc}")
                    elements.append(Paragraph(f"图片: {img_desc}", self.styles['ChineseNormal']))
        except Exception as e:
            logger.error(f"添加特殊图片引用时出错: {img_desc}, 错误: {e}")
            elements.append(Paragraph(f"处理图片时出错: {img_desc} - {e}", 
                                     self.styles['ChineseNormal']))
    
    def _add_markdown_image(self, elements: List, img_path: str, alt_text: str = ""):
        """处理Markdown图片语法并添加图片"""
        try:
            logger.debug(f"处理图片路径: {img_path}")
            
            # 处理图片路径
            # 如果路径以相对路径开始，基于输入目录解析
            if not os.path.isabs(img_path):
                # 查找文件组中的匹配图片
                img_basename = os.path.basename(img_path)
                full_img_path = None
                
                # 输出调试信息
                logger.debug(f"查找图片文件: {img_path}, 基础名称: {img_basename}")
                if hasattr(self, 'current_group') and self.current_group:
                    logger.debug(f"当前处理组: {self.current_group.group_key}, 图片数量: {len(self.current_group.image_files) if self.current_group.image_files else 0}")
                if hasattr(self, 'input_dir'):
                    logger.debug(f"当前输入目录: {self.input_dir}")
                
                # 尝试直接使用路径
                candidate_paths = []
                if hasattr(self, 'input_dir'):
                    candidate_paths.append(os.path.join(self.input_dir, img_path))
                    # 尝试img子目录
                    candidate_paths.append(os.path.join(self.input_dir, 'img', img_basename))
                
                for candidate_path in candidate_paths:
                    logger.debug(f"尝试路径: {candidate_path}")
                    if os.path.exists(candidate_path):
                        full_img_path = candidate_path
                        logger.debug(f"找到图片: {full_img_path}")
                        break
                
                # 如果直接使用路径不存在，遍历当前处理的文件组中的图片
                if not full_img_path and hasattr(self, 'current_group') and self.current_group and self.current_group.image_files:
                    logger.debug(f"在文件组的 {len(self.current_group.image_files)} 张图片中查找: {img_basename}")
                    for img_file in self.current_group.image_files:
                        logger.debug(f"检查图片: {img_file.path}, 基础名称: {os.path.basename(img_file.path)}")
                        if os.path.basename(img_file.path) == img_basename or img_file.path.endswith(img_path.split('/')[-1]):
                            full_img_path = img_file.path
                            logger.debug(f"在文件组中找到匹配的图片: {full_img_path}")
                            break
                
                if full_img_path:
                    img_path = full_img_path
                    logger.debug(f"最终使用的图片路径: {img_path}")
                else:
                    logger.warning(f"无法找到图片文件: {img_path}")
                    elements.append(Paragraph(f"图片未找到: {img_path}", self.styles['ChineseNormal']))
                    return
            
            # 确保文件存在
            if not os.path.exists(img_path):
                logger.warning(f"图片文件不存在: {img_path}")
                elements.append(Paragraph(f"图片文件不存在: {img_path}", self.styles['ChineseNormal']))
                return
            
            # 获取图像尺寸
            try:
                img = PILImage.open(img_path)
                img_width, img_height = img.size
                logger.debug(f"图片尺寸: {img_width}x{img_height}")
                
                # 计算适当的展示尺寸
                max_width = self.width - self.left_margin - self.right_margin - 0.5*inch
                if img_width > max_width:
                    scale = max_width / img_width
                    display_width = max_width
                    display_height = img_height * scale
                else:
                    display_width = img_width
                    display_height = img_height
                
                logger.debug(f"展示尺寸: {display_width}x{display_height}")
                
                # 释放图像对象
                img.close()
                
                # 添加图片间距
                elements.append(Spacer(0, 0.2*inch))
                
                # 创建ReportLab图像对象
                reportlab_image = Image(img_path, width=display_width, height=display_height)
                elements.append(reportlab_image)
                logger.debug(f"已添加图片到文档: {img_path}")
                
                # 添加图片说明
                if alt_text:
                    elements.append(Paragraph(alt_text, self.styles['ChineseCaption']))
                
                # 添加图片后间距
                elements.append(Spacer(0, 0.2*inch))
                
            except Exception as img_error:
                logger.error(f"处理图片时出错: {img_path}, 错误: {img_error}")
                elements.append(Paragraph(f"无法加载图片: {img_path} - {img_error}", 
                                        self.styles['ChineseNormal']))
                elements.append(Spacer(0, 0.1*inch))
                
        except Exception as e:
            logger.error(f"添加图片时出错: {img_path}, 错误: {e}")
            elements.append(Paragraph(f"处理图片时出错: {img_path} - {e}", 
                                    self.styles['ChineseNormal']))
            elements.append(Spacer(0, 0.1*inch))
    
    def _add_image_content(self, elements: List, image_file: FileMetadata):
        """
        添加圖像文件內容
        
        Args:
            elements: 文檔元素列表
            image_file: 圖像文件元數據
        """
        try:
            # 檢查文件是否存在
            if not os.path.exists(image_file.path):
                logger.warning(f"圖像文件不存在: {image_file.path}")
                elements.append(Paragraph(f"圖像文件不存在: {image_file.basename}", 
                                         self.styles['BodyText']))
                return
                
            # 檢查文件大小
            file_size = os.path.getsize(image_file.path)
            if file_size == 0:
                logger.warning(f"圖像文件為空: {image_file.path}")
                elements.append(Paragraph(f"圖像文件為空: {image_file.basename}", 
                                         self.styles['BodyText']))
                return
                
            logger.info(f"處理圖像: {image_file.path} (大小: {file_size} 字節)")
            
            # 獲取圖像尺寸和格式
            try:
                img = PILImage.open(image_file.path)
                img_width, img_height = img.size
                img_format = img.format
                
                # 計算圖像展示尺寸（縮放到適合頁面寬度）
                max_width = self.width - self.left_margin - self.right_margin - 0.5*inch
                if img_width > max_width:
                    scale = max_width / img_width
                    display_width = max_width
                    display_height = img_height * scale
                else:
                    display_width = img_width
                    display_height = img_height
                
                # 釋放圖像對象
                img.close()
                
                # 添加圖片間距
                elements.append(Spacer(0, 0.2*inch))
                
                # 創建ReportLab圖像對象
                reportlab_image = Image(image_file.path, width=display_width, height=display_height)
                elements.append(reportlab_image)
                
                # 提取並添加圖片說明（如果有）
                caption = ""
                basename = os.path.basename(image_file.path)
                if hasattr(image_file, 'caption') and image_file.caption:
                    caption = image_file.caption
                else:
                    # 從文件名提取可能的說明內容
                    caption_match = re.search(r'_([^_\.]+)\.\w+$', basename)
                    if caption_match:
                        caption = caption_match.group(1).replace('_', ' ')
                
                # 添加圖片說明
                if caption:
                    # 将ImageCaption改为ChineseCaption
                    elements.append(Paragraph(f"圖: {caption}", self.styles['ChineseCaption']))
                else:
                    # 将ImageCaption改为ChineseCaption
                    elements.append(Paragraph(f"圖: {basename}", self.styles['ChineseCaption']))
                
                # 添加圖片後間距
                elements.append(Spacer(0, 0.3*inch))
                
            except Exception as img_error:
                logger.error(f"創建ReportLab圖像對象失敗: {image_file.path}, 錯誤: {img_error}")
                elements.append(Paragraph(f"無法載入圖像: {image_file.basename} - {img_error}", 
                                         self.styles['BodyText']))
                elements.append(Spacer(0, 0.2*inch))
                
        except Exception as e:
            logger.error(f"添加圖像內容時出錯: {e}")
            elements.append(Paragraph(f"處理圖像時出錯: {image_file.basename} - {e}", 
                                     self.styles['BodyText']))
            elements.append(Spacer(0, 0.2*inch))
    
    def _detect_encoding(self, file_path: Path) -> str:
        """
        檢測文本文件編碼
        
        Args:
            file_path: 文件路徑
            
        Returns:
            str: 檢測到的編碼
        """
        # 首先檢查UTF-8 BOM
        try:
            with open(file_path, 'rb') as f:
                raw = f.read(4)
                if raw.startswith(b'\xef\xbb\xbf'):
                    return 'utf-8-sig'
        except Exception:
            pass

        # 使用chardet進行檢測
        if CHARDET_AVAILABLE:
            try:
                with open(file_path, 'rb') as f:
                    raw = f.read()
                    result = chardet.detect(raw)
                    if result['encoding'] and result['confidence'] > 0.7:
                        return result['encoding']
            except Exception:
                pass
        
        # 嘗試常見中文編碼
        encodings = [
            'utf-8',
            'gb18030',  # 最全面的中文編碼
            'gbk',      # Windows 默認中文編碼
            'gb2312',   # 簡體中文
            'big5',     # 繁體中文
            'big5hkscs',# 香港繁體中文
            'cp950',    # Windows 繁體中文
            'cp936',    # Windows 簡體中文
            'shift-jis',# 日文
            'euc-jp',   # 日文
            'euc-kr',   # 韓文
            'latin1',   # 西歐
            'ascii'     # ASCII
        ]
        
        # 讀取文件的前4KB來測試編碼
        try:
            with open(file_path, 'rb') as f:
                raw = f.read(4096)
                
            for enc in encodings:
                try:
                    raw.decode(enc)
                    return enc
                except Exception:
                    continue
        except Exception:
            pass
        
        # 如果所有嘗試都失敗，返回系統默認編碼
        import locale
        return locale.getpreferredencoding()
    
    def _clean_text(self, text: str) -> str:
        """
        清理文本，處理特殊字符和編碼問題
        
        Args:
            text: 輸入文本
            
        Returns:
            str: 清理後的文本
        """
        if not text:
            return ""
        
        # 移除控制字符
        text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
        
        # 轉義特殊HTML字符
        text = text.replace('&', '&amp;')
        text = text.replace('<', '&lt;')
        text = text.replace('>', '&gt;')
        
        return text
    
    def _setup_doc_template(self, file_path):
        """设置文档模板及样式"""
        self.doc = SimpleDocTemplate(
            file_path,
            pagesize=A4,
            rightMargin=36,
            leftMargin=36,
            topMargin=50,
            bottomMargin=50
        )
        
        # 创建样式表
        self.styles = getSampleStyleSheet()
        
        # 修复：检查样式是否已存在
        # 只有当样式不存在时才添加新样式
        if 'ChineseTitle' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseTitle',
                parent=self.styles['Title'],
                fontName='STSong-Light',
                fontSize=18,
                leading=22,
                alignment=1,
                spaceAfter=12
            ))
        
        if 'ChineseHeading1' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseHeading1',
                parent=self.styles['Heading1'],
                fontName='STSong-Light',
                fontSize=16,
                leading=20,
                spaceAfter=10
            ))
        
        if 'ChineseHeading2' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseHeading2',
                parent=self.styles['Heading2'],
                fontName='STSong-Light',
                fontSize=14,
                leading=18,
                spaceAfter=8
            ))
        
        if 'ChineseNormal' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseNormal',
                parent=self.styles['Normal'],
                fontName='STSong-Light',
                fontSize=12,
                leading=16,
                spaceAfter=6
            ))
        
        if 'ChineseCaption' not in self.styles:
            self.styles.add(ParagraphStyle(
                name='ChineseCaption',
                parent=self.styles['Normal'],
                fontName='STSong-Light',
                fontSize=10,
                leading=14,
                alignment=1,  # 居中
                spaceAfter=12,
                spaceBefore=6
            ))
            
        return self.doc


# 4. 批處理控制系統
class BatchProcessor:
    """PDF批量處理控制系統"""
    
    def __init__(self, 
                 input_dir: Union[str, Path], 
                 output_dir: Union[str, Path] = None,
                 recursive: bool = True,
                 max_workers: int = None,
                 batch_html_mode: bool = False):
        """
        初始化批處理器
        
        Args:
            input_dir: 輸入目錄
            output_dir: 輸出目錄
            recursive: 是否遞歸處理子目錄
            max_workers: 最大工作線程數
            batch_html_mode: 是否使用批處理HTML模式（所有文件一次處理）
        """
        self.input_dir = Path(input_dir)
        
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = self.input_dir / DEFAULT_OUTPUT_DIR
        
        # 確保輸出目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.recursive = recursive
        self.batch_html_mode = batch_html_mode
        
        # 設置線程數
        if max_workers is None:
            # 使用默認值 (CPU核心數 x 2)
            self.max_workers = multiprocessing.cpu_count() * 2
        else:
            self.max_workers = max_workers
        
        # 創建文件掃描器
        self.file_scanner = FileScanner(self.input_dir, self.recursive)
        
        # 初始化PDF生成器（之後會被設置）
        self._pdf_generator = None
        
        # 将绝对路径存储为字符串，方便后续使用
        self.input_dir_str = str(self.input_dir.absolute())
        self.output_dir_str = str(self.output_dir.absolute())
        
        # 處理狀態
        self.processing_complete = False
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.result_files = []
        
        # 記錄失敗的組
        self.failed_groups = []
        
        # 處理進度文件名
        self.progress_file = "pdf_generation_progress.json"
    
    @property
    def pdf_generator(self):
        """獲取PDF生成器，如果尚未設置則創建默認實例"""
        if self._pdf_generator is None:
            # 默認創建基本PDF生成器
            self._pdf_generator = PDFGenerator(self.output_dir)
        return self._pdf_generator
    
    @pdf_generator.setter
    def pdf_generator(self, generator):
        """設置PDF生成器"""
        self._pdf_generator = generator

    def process_batch_html(self, file_groups: List[FileGroup]) -> Optional[Path]:
        """
        使用批處理HTML模式處理文件組
        
        生成一個包含所有文件的大型HTML文件，然後由用戶一次性保存為PDF
        
        Args:
            file_groups: 要處理的文件組列表
            
        Returns:
            Optional[Path]: 生成的HTML文件路徑
        """
        if not file_groups:
            logger.warning("沒有可處理的文件組")
            return None
            
        try:
            return self.pdf_generator.process_batch_html(file_groups)
        except Exception as e:
            logger.error(f"生成批量HTML時出錯: {e}")
            import traceback
            logger.debug(f"錯誤詳情: {traceback.format_exc()}")
            return None

    def process(self) -> List[Path]:
        """
        執行批處理流程
        
        Returns:
            List[Path]: 生成的PDF文件路徑列表
        """
        # 掃描並配對文件
        logger.info(f"開始批處理，掃描目錄: {self.input_dir}")
        text_files, image_files = self.file_scanner.scan()
        
        # 配對文件
        matcher = FileMatcher(text_files, image_files)
        file_groups = matcher.match_files()
        
        if not file_groups:
            logger.warning("沒有找到可配對的文件")
            return []
        
        # 如果使用批處理HTML模式，則一次處理所有文件
        if self.batch_html_mode:
            logger.info(f"使用批處理HTML模式處理 {len(file_groups)} 個文件組")
            html_path = self.process_batch_html(file_groups)
            if html_path:
                logger.info(f"成功生成批量HTML: {html_path}")
                return [html_path]
            else:
                logger.error("生成批量HTML失敗")
                return []
        
        # 使用多線程處理文件組
        start_time = time.time()
        result_files = []
        success_count = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_group = {
                executor.submit(self._process_group, group): group 
                for group in file_groups
            }
            
            total = len(file_groups)
            processed = 0
            
            for future in as_completed(future_to_group):
                group = future_to_group[future]
                try:
                    pdf_path = future.result()
                    if pdf_path:
                        result_files.append(pdf_path)
                        success_count += 1
                except Exception as e:
                    logger.error(f"處理文件組時出錯 '{group.group_key}': {e}")
                
                processed += 1
                progress = (processed / total) * 100
                logger.info(f"進度: {processed}/{total} ({progress:.1f}%)")
        
        # 輸出統計結果
        elapsed = time.time() - start_time
        logger.info(f"批處理完成，共處理 {len(file_groups)} 個組，成功: {success_count}，失敗: {len(file_groups) - success_count}，用時: {elapsed:.2f} 秒")
        
        return result_files

    def _process_group(self, file_group: FileGroup) -> Optional[Path]:
        """处理单个文件组，生成PDF"""
        try:
            # 首先读取文本文件内容以获取标题
            if not file_group.text_files:
                logger.warning(f"组 {file_group.group_key} 没有文本文件")
                return None

            first_text_file = file_group.text_files[0]
            title = None
            
            try:
                with open(first_text_file.path, 'r', encoding='utf-8') as f:
                    first_lines = [line.strip() for line in f.readlines(2000)][:20]
                    for line in first_lines:
                        if line.startswith('标题:') or line.startswith('標題:'):
                            title = line.split(':', 1)[1].strip()
                            break
                        elif line and len(line) > 3 and len(line) < 200:
                            # 排除看起来像URL或其他非标题内容的行
                            if not line.startswith(('http', 'www', '#', '/')):
                                title = line
                                break
            except Exception as e:
                logger.warning(f"读取文件获取标题时出错: {e}")

            # 如果找到标题，使用标题作为文件名
            if title:
                # 清理标题中的非法字符
                safe_title = re.sub(r'[<>:"/\\|?*]', '_', title)
                # 限制文件名长度
                if len(safe_title.encode('utf-8')) > 200:  # 限制字节长度
                    safe_title = safe_title[:50]  # 取前50个字符
                
                # 构建输出文件路径
                output_file = os.path.join(self.output_dir, f"{safe_title}.pdf")
            else:
                # 如果没有找到标题，使用默认的group_key
                output_file = os.path.join(self.output_dir, f"{file_group.group_key}.pdf")

            # 使用更新后的PDFGenerator初始化方式
            pdf_generator = PDFGenerator(
                output_file=output_file,
                article_id=file_group.group_key,
                title=title or file_group.group_key
            )
            
            # 设置输入目录路径
            pdf_generator.input_dir = self.input_dir
            # 设置当前处理的文件组
            pdf_generator.current_group = file_group
            
            # 生成PDF
            result = pdf_generator.generate_pdf(file_group)
            
            # 如果生成成功，返回实际的输出文件路径
            if result and os.path.exists(pdf_generator.output_file):
                return pdf_generator.output_file
            return None
            
        except Exception as e:
            logger.error(f"处理文件组时出错 '{file_group.group_key}': {e}")
            # 记录详细堆栈跟踪以便调试
            import traceback
            logger.debug(f"错误详情: {traceback.format_exc()}")
            raise e

    def resilient_process(self, file_groups: List[FileGroup], 
                         batch_size: int = 50, 
                         max_retries: int = 3) -> List[Path]:
        """
        實現彈性批處理邏輯，包含錯誤恢復
        
        Args:
            file_groups: 待處理的文件組列表
            batch_size: 每個批次的文件組數量
            max_retries: 失敗後的最大重試次數
            
        Returns:
            List[Path]: 生成的PDF文件路徑列表
        """
        start_time = time.time()
        logger.info(f"開始彈性批處理，共 {len(file_groups)} 個文件組")
        
        self.result_files = []
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        self.failed_groups = []
        
        # 分批處理
        for i in range(0, len(file_groups), batch_size):
            batch = file_groups[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(file_groups) - 1) // batch_size + 1
            logger.info(f"處理批次 {batch_num}/{total_batches}")
            
            try:
                # 處理當前批次
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    future_to_group = {
                        executor.submit(self._process_group, group): group 
                        for group in batch
                    }
                    
                    for future in as_completed(future_to_group):
                        group = future_to_group[future]
                        try:
                            pdf_path = future.result()
                            if pdf_path:
                                self.result_files.append(pdf_path)
                                self.success_count += 1
                        except Exception as e:
                            logger.error(f"處理文件組時出錯 '{group.group_key}': {e}")
                            self.error_count += 1
                            self.failed_groups.append(group)
                        
                        self.processed_count += 1
                        progress = (self.processed_count / len(file_groups)) * 100
                        logger.info(f"進度: {self.processed_count}/{len(file_groups)} ({progress:.1f}%)")
                
                # 定期垃圾回收
                if batch_num % 2 == 0:  # 每兩個批次執行一次
                    import gc
                    gc.collect()
                    logger.debug(f"在批次 {batch_num} 後執行垃圾回收")
                
            except Exception as e:
                logger.error(f"處理批次 {batch_num} 時出錯: {e}")
                
                # 記錄失敗的批次中的所有組
                self.failed_groups.extend(batch)
        
        # 重試失敗的組
        if self.failed_groups and max_retries > 0:
            logger.info(f"開始重試 {len(self.failed_groups)} 個失敗的文件組...")
            
            for retry in range(max_retries):
                if not self.failed_groups:
                    break
                
                logger.info(f"第 {retry + 1} 次重試，剩餘 {len(self.failed_groups)} 個失敗組")
                
                retry_groups = self.failed_groups
                self.failed_groups = []  # 重置失敗列表
                
                # 使用較小的批次大小
                smaller_batch_size = max(1, batch_size // 2)
                
                for i in range(0, len(retry_groups), smaller_batch_size):
                    sub_batch = retry_groups[i:i+smaller_batch_size]
                    
                    try:
                        # 單線程處理重試批次，提高穩定性
                        for group in sub_batch:
                            try:
                                pdf_path = self._process_group(group)
                                if pdf_path:
                                    self.result_files.append(pdf_path)
                                    self.success_count += 1
                                    self.error_count -= 1  # 修正之前的錯誤計數
                            except Exception as e:
                                logger.error(f"重試處理組 '{group.group_key}' 時仍然出錯: {e}")
                                self.failed_groups.append(group)
                    except Exception as e:
                        logger.error(f"重試批次處理時出錯: {e}")
                        self.failed_groups.extend(sub_batch)
            
            if not self.failed_groups:
                logger.info("所有失敗組重試成功")
            else:
                logger.warning(f"仍有 {len(self.failed_groups)} 個組無法處理")
        
        # 完成處理
        self.processing_complete = True
        elapsed_time = time.time() - start_time
        
        logger.info(f"彈性批處理完成，共處理 {len(file_groups)} 個組，"
                    f"成功: {self.success_count}，失敗: {len(self.failed_groups)}，"
                    f"用時: {elapsed_time:.2f} 秒")
        
        return self.result_files


# 命令行參數解析與主程序
def parse_arguments():
    """
    解析命令行參數 (已廢棄,僅為向後兼容)
    """
    import warnings
    warnings.warn(
        "parse_arguments() 函數已廢棄，請直接使用 main() 函數中的參數解析",
        DeprecationWarning, 
        stacklevel=2
    )
    
    # 使用與main()相同的參數解析邏輯
    parser = argparse.ArgumentParser(
        description="批量PDF生成工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # 必要參數
    parser.add_argument('input_dir', 
                        help='輸入目錄路徑，包含要處理的文本和圖像文件')
    parser.add_argument('output_dir', nargs='?', default=None,
                        help='輸出PDF文件的目錄路徑')
    
    # 基本參數
    parser.add_argument('-r', '--recursive', 
                        action='store_true', 
                        default=True,
                        help='遞歸處理子目錄')
    
    parser.add_argument('-w', '--workers', 
                        type=int, 
                        default=4,
                        help='處理線程數量')
    
    parser.add_argument('-v', '--verbose', 
                        action='store_true', 
                        help='輸出詳細日誌')
    
    # 日誌參數
    parser.add_argument('--log-level', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default=None,
                        help='日誌級別')
    
    parser.add_argument('--log-file', 
                        help='日誌文件路徑')
    
    # 增強參數
    parser.add_argument('--match-type', 
                        choices=['id', 'name', 'content', 'hybrid'], 
                        default='hybrid',
                        help='文件匹配策略')
    
    parser.add_argument('--use-optimizer', 
                        action='store_true', 
                        help='使用文件搜索優化器')
    
    parser.add_argument('--similarity-threshold', 
                        type=float, 
                        default=0.7,
                        help='文件名相似度閾值 (0-1)')
    
    parser.add_argument('--template', 
                        choices=['default', 'academic', 'report'], 
                        default='default',
                        help='PDF模板樣式')
    
    parser.add_argument('--font-path', 
                        type=str,
                        default=None,
                        help='指定中文字體文件路徑，如果提供，將優先使用該字體')
    
    parser.add_argument('--use-pdfkit', 
                        action='store_true',
                        default=True,
                        help='使用pdfkit而非reportlab生成PDF，更好地支持中文（需要安裝wkhtmltopdf）')
    
    parser.add_argument('--use-reportlab', 
                        action='store_true',
                        help='強制使用reportlab而非pdfkit生成PDF')
    
    parser.add_argument('--use-html-browser', 
                        action='store_true',
                        help='使用瀏覽器方式生成PDF，打開HTML並讓用戶手動保存為PDF（最佳中文支持）')
    
    parser.add_argument('--batch-html-mode', 
                        action='store_true',
                        help='生成一個包含所有文件的HTML文件，然後一次性保存為PDF（適用於大量文件的中文處理）')
    
    parser.add_argument('--use-arial-unicode', 
                        action='store_true',
                        default=True,
                        help='使用系統的Arial Unicode字體，最大程度解決中文亂碼問題（MacOS）')
    
    parser.add_argument('--compression', 
                        type=int, 
                        choices=range(0, 5),
                        default=2,
                        help='圖像壓縮級別 (0-4)')
    
    # 批處理相關參數
    parser.add_argument('--batch-size', 
                        type=int, 
                        default=50,
                        help='批量處理大小，0表示一次處理所有文件')
    
    parser.add_argument('--max-retries', 
                        type=int, 
                        default=3, 
                        help='失敗後最大重試次數')
    
    parser.add_argument('--resilient', 
                        action='store_true', 
                        help='使用彈性處理模式，提供更好的錯誤恢復')
    
    parser.add_argument('--buffer-size', 
                        type=int, 
                        default=8192,
                        help='I/O緩衝區大小')
    
    args = parser.parse_args()
    
    # 處理輸出目錄
    if not args.output_dir:
        args.output_dir = os.path.join(args.input_dir, DEFAULT_OUTPUT_DIR)
    
    # 處理日誌級別
    if args.log_level is None:
        args.log_level = 'DEBUG' if args.verbose else 'INFO'
    
    return args


def setup_logging(log_level, log_file=None):
    """
    配置日誌系統
    
    Args:
        log_level: 日誌級別 (DEBUG, INFO, WARNING, ERROR)
        log_file: 日誌文件路徑 (可選)
    """
    # 設置日誌級別
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'無效的日誌級別: {log_level}')
    
    # 基本配置
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 如果指定了日誌文件，添加文件處理器
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logging.getLogger().addHandler(file_handler)
    
    logger.debug(f"日誌系統已配置，級別: {log_level}")


def main():
    """
    程序入口點，解析命令行參數並執行批處理
    
    Returns:
        int: 程序退出代碼
    """
    # 解析命令行參數
    parser = argparse.ArgumentParser(
        description="批量PDF生成工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # 必要參數
    parser.add_argument('input_dir', 
                        help='輸入目錄路徑，包含要處理的文本和圖像文件')
    parser.add_argument('output_dir', nargs='?', default=None,
                        help='輸出PDF文件的目錄路徑')
    
    # 基本參數
    parser.add_argument('-r', '--recursive', 
                        action='store_true', 
                        default=True,
                        help='遞歸處理子目錄')
    
    parser.add_argument('-w', '--workers', 
                        type=int, 
                        default=4,
                        help='處理線程數量')
    
    parser.add_argument('-v', '--verbose', 
                        action='store_true', 
                        help='輸出詳細日誌')
    
    # 日誌參數
    parser.add_argument('--log-level', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
                        default=None,
                        help='日誌級別')
    
    parser.add_argument('--log-file', 
                        default='batch_pdf_generator.log',
                        help='日誌文件路徑')
    
    # 額外參數
    parser.add_argument('--skip-existing', 
                        action='store_true',
                        help='跳過已存在的PDF文件')
    
    parser.add_argument('--article-id', 
                        help='僅處理指定ID的文章')
    
    args = parser.parse_args()
    
    # 處理輸出目錄，如果沒有提供，使用相對於輸入目錄的路徑
    if not args.output_dir:
        args.output_dir = os.path.join(args.input_dir, DEFAULT_OUTPUT_DIR)
    
    # 配置日誌級別
    if args.log_level is None:
        args.log_level = 'DEBUG' if args.verbose else 'INFO'
        
    # 配置日誌
    setup_logging(args.log_level, args.log_file)
    
    # 創建輸出目錄
    os.makedirs(args.output_dir, exist_ok=True)
    
    try:
        # 顯示系統信息
        logger.info(f"批量PDF生成工具啟動")
        logger.info(f"Python版本: {sys.version}")
        logger.info(f"操作系統: {os.name} - {sys.platform}")
        logger.info(f"工作目錄: {os.getcwd()}")
        logger.info(f"輸入目錄: {os.path.abspath(args.input_dir)}")
        logger.info(f"輸出目錄: {os.path.abspath(args.output_dir)}")
        logger.info(f"遞歸處理: {'是' if args.recursive else '否'}")
        logger.info(f"工作線程數: {args.workers}")
        
        # 檢查 ReportLab 和 PIL 版本
        import reportlab
        logger.info(f"ReportLab版本: {reportlab.Version}")
        logger.info(f"PIL/Pillow版本: {PILImage.__version__ if hasattr(PILImage, '__version__') else '未知'}")
        
        # 檢查圖像模式和字體支持
        logger.info(f"支持的圖像模式: {', '.join(PILImage.MODES)}")
        
        # 檢查輸入目錄內容
        logger.info(f"檢查輸入目錄: {args.input_dir}")
        root_dir = Path(args.input_dir)
        if not root_dir.exists() or not root_dir.is_dir():
            logger.error(f"輸入目錄不存在或不是有效目錄: {args.input_dir}")
            return 1
            
        # 顯示目錄中的文件類型分佈
        file_extensions = {}
        for file_path in root_dir.glob("**/*" if args.recursive else "*"):
            if file_path.is_file():
                ext = file_path.suffix.lower()
                file_extensions[ext] = file_extensions.get(ext, 0) + 1
                
        logger.info(f"輸入目錄文件類型分佈: {file_extensions}")
        
        # 初始化批處理器
        processor = BatchProcessor(
            args.input_dir,
            args.output_dir,
            recursive=args.recursive,
            max_workers=args.workers
        )
        
        # 掃描文件並配對
        start_time = time.time()
        logger.info(f"開始掃描目錄: {args.input_dir}")
        
        text_files, image_files = processor.file_scanner.scan()
        logger.info(f"掃描完成: 找到 {len(text_files)} 個文本文件和 {len(image_files)} 個圖像文件")
        
        # 如果指定了article_id參數，則只處理該ID的文件
        if args.article_id:
            logger.info(f"僅處理ID為 {args.article_id} 的文章")
            original_text_count = len(text_files)
            original_image_count = len(image_files)
            
            text_files = [f for f in text_files if f.match_key == args.article_id]
            image_files = [f for f in image_files if f.match_key == args.article_id]
            
            logger.info(f"篩選後: {len(text_files)}/{original_text_count} 個文本文件和 {len(image_files)}/{original_image_count} 個圖像文件")
            
            # 檢查是否找到了文件
            if not text_files:
                logger.warning(f"未找到ID為 {args.article_id} 的文本文件")
                
                # 嘗試搜索其他可能的ID格式
                alternative_ids = []
                for f in processor.file_scanner.text_files:
                    if args.article_id in f.basename:
                        alternative_ids.append(f.match_key)
                
                if alternative_ids:
                    logger.info(f"找到可能相關的ID: {', '.join(set(alternative_ids))}")
            
            if not image_files:
                logger.warning(f"未找到ID為 {args.article_id} 的圖像文件")
        
        # 配對文件
        file_matcher = FileMatcher(text_files, image_files)
        file_groups = file_matcher.match_files()
        
        if not file_groups:
            logger.warning("未找到可配對的文件組")
            return 0
        
        logger.info(f"找到 {len(file_groups)} 個可處理的文件組")
        
        # 檢查每個組的文件
        for i, group in enumerate(file_groups):
            logger.info(f"組 {i+1}: 鍵={group.group_key}, 文本文件={len(group.text_files)}, 圖像文件={len(group.image_files)}")
            
            # 列出文本文件
            for tf in group.text_files:
                logger.debug(f"  文本: {tf.path}")
                
            # 列出圖像文件
            for imf in group.image_files:
                logger.debug(f"  圖像: {imf.path}")
                # 檢查圖像文件是否存在
                if not os.path.exists(imf.path):
                    logger.warning(f"  圖像文件不存在: {imf.path}")
            
            # 如果已存在對應的PDF且設置了--skip-existing，則跳過
            if args.skip_existing:
                pdf_path = os.path.join(args.output_dir, group.output_filename)
                if os.path.exists(pdf_path):
                    logger.info(f"PDF已存在，跳過: {pdf_path}")
                    file_groups[i] = None
        
        # 過濾掉None值
        file_groups = [g for g in file_groups if g is not None]
        logger.info(f"過濾後，將處理 {len(file_groups)} 個文件組")
        
        # 使用標準處理模式
        pdf_files = processor.process()
        
        # 結果報告
        elapsed_time = time.time() - start_time
        logger.info(f"處理完成，共生成 {len(pdf_files)} 個PDF文件")
        logger.info(f"總用時: {elapsed_time:.2f} 秒")
        
        # 如果成功生成了PDF，輸出文件列表
        if pdf_files:
            logger.info("已生成的PDF文件:")
            for pdf_file in pdf_files:
                file_size = os.path.getsize(pdf_file) if os.path.exists(pdf_file) else 0
                file_size_kb = file_size / 1024
                logger.info(f"  {pdf_file} ({file_size_kb:.1f} KB)")
            
            # 如果只有一個PDF，建議下一步操作
            if len(pdf_files) == 1:
                logger.info(f"成功生成PDF: {pdf_files[0]}")
                logger.info(f"您可以使用PDF閱讀器打開該文件進行查看")
        
        return 0
    
    except KeyboardInterrupt:
        logger.warning("用戶中斷處理")
        return 130
    except Exception as e:
        logger.critical(f"處理過程中發生嚴重錯誤: {e}", exc_info=True)
        import traceback
        logger.critical(f"錯誤詳情:\n{traceback.format_exc()}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
