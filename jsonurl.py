import requests
import json
from bs4 import BeautifulSoup
import os
import time
import re
import logging
from urllib.parse import urlparse, urljoin, quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
import threading
import hashlib
import shutil
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union, Tuple, Any, Set, Callable, Protocol, TypeVar, Generic
import traceback
from abc import ABC, abstractmethod
from enum import Enum, auto
import io
from contextlib import contextmanager
import signal
import sys

#-------------------------------------------------------
# 配置、類型與常量定義
#-------------------------------------------------------

# 定義網頁內容類型枚舉
class ContentType(Enum):
    UNKNOWN = auto()
    ARTICLE = auto()
    BLOG = auto()
    TECHNICAL = auto()
    TUTORIAL = auto()
    NEWS = auto()
    PAYWALL = auto()

@dataclass
class ScrapingConfig:
    """配置參數數據類"""
    input_file: str
    output_dir: str = "scraped_articles"
    max_workers: int = 5
    delay: float = 1.0
    download_images: bool = True
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 2.0
    user_agent_rotation: bool = True
    respect_robots_txt: bool = True
    verify_ssl: bool = True
    proxy: Optional[str] = None
    image_download_timeout: int = 20
    max_image_workers: int = 3
    min_content_length: int = 500
    paywall_threshold: int = 30
    
    def __post_init__(self):
        """驗證配置參數的有效性"""
        if self.max_workers < 1:
            raise ValueError("max_workers必須為正整數")
        if self.delay < 0:
            raise ValueError("delay必須為非負數")
        if self.timeout < 1:
            raise ValueError("timeout必須為正整數")
        if self.max_retries < 0:
            raise ValueError("max_retries必須為非負整數")
        if self.backoff_factor <= 0:
            raise ValueError("backoff_factor必須為正數")
        if self.paywall_threshold < 0:
            raise ValueError("paywall_threshold必須為非負整數")

@dataclass
class Article:
    """文章數據模型"""
    url: str
    title: str = ""
    id: str = ""
    author: str = ""
    content: str = ""
    html: str = ""
    publish_date: str = ""
    is_paywall: bool = False
    content_type: ContentType = ContentType.UNKNOWN
    images: List[Dict[str, str]] = field(default_factory=list)
    text_length: int = 0
    download_time: float = 0.0
    error: Optional[str] = None
    paywall_score: int = 0  # 新增: 付費牆得分
    paywall_details: Dict[str, Any] = field(default_factory=dict)  # 新增: 付費牆詳細信息
    
    def __post_init__(self):
        """初始化後自動生成ID（如果未提供）"""
        if not self.id:
            self.id = hashlib.md5(self.url.encode()).hexdigest()[:8]

@dataclass
class ScrapingStats:
    """爬蟲統計數據"""
    start_time: float = 0.0
    end_time: float = 0.0
    total_urls: int = 0
    successful_urls: int = 0
    failed_urls: int = 0
    paywall_count: int = 0
    total_images: int = 0
    successful_images: int = 0
    failed_images: int = 0
    total_bytes: int = 0
    average_response_time: float = 0.0
    domains: Dict[str, int] = field(default_factory=dict)
    errors: Dict[str, int] = field(default_factory=dict)
    
    def calculate_duration(self) -> float:
        """計算總執行時間（秒）"""
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        return 0.0
    
    def calculate_success_rate(self) -> float:
        """計算URL爬取成功率"""
        if self.total_urls == 0:
            return 0.0
        return (self.successful_urls / self.total_urls) * 100
    
    def calculate_image_success_rate(self) -> float:
        """計算圖片下載成功率"""
        if self.total_images == 0:
            return 0.0
        return (self.successful_images / self.total_images) * 100
    
    def update_domain_stats(self, url: str):
        """更新域名統計"""
        domain = urlparse(url).netloc
        self.domains[domain] = self.domains.get(domain, 0) + 1
    
    def update_error_stats(self, error_type: str):
        """更新錯誤統計"""
        self.errors[error_type] = self.errors.get(error_type, 0) + 1

#-------------------------------------------------------
# 日誌配置
#-------------------------------------------------------

class LoggerFactory:
    """建立和配置日誌記錄器的工廠類"""
    
    @staticmethod
    def create_logger(name: str = __name__, 
                     level: int = logging.INFO, 
                     log_file: str = "scraper.log",
                     console: bool = True) -> logging.Logger:
        """
        創建並配置日誌記錄器
        
        參數:
            name: 記錄器名稱
            level: 日誌級別
            log_file: 日誌文件路徑
            console: 是否同時輸出到控制台
            
        返回:
            配置好的Logger實例
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # 防止重複添加handler
        if not logger.handlers:
            # 格式
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
            
            # 文件處理器
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            # 可選的控制台處理器
            if console:
                console_handler = logging.StreamHandler()
                console_handler.setLevel(level)
                console_handler.setFormatter(formatter)
                logger.addHandler(console_handler)
        
        return logger

# 創建主日誌記錄器
logger = LoggerFactory.create_logger("web_scraper")

#-------------------------------------------------------
# 異常類定義
#-------------------------------------------------------

class ScraperException(Exception):
    """爬蟲基礎異常類"""
    pass

class NetworkException(ScraperException):
    """網路連接相關異常"""
    pass

class ParseException(ScraperException):
    """內容解析相關異常"""
    pass

class PaywallException(ScraperException):
    """付費牆檢測異常"""
    pass

class RateLimitException(NetworkException):
    """速率限制異常"""
    pass

class InputOutputException(ScraperException):
    """輸入輸出相關異常"""
    pass

class InterfaceContractException(ScraperException):
    """接口契約違反異常"""
    pass

#-------------------------------------------------------
# 工具函數
#-------------------------------------------------------

class TextUtils:
    """文本處理工具類"""
    
    @staticmethod
    def clean_text(text: str) -> str:
        """
        清理文本中的HTML標籤和特殊字符
        
        參數:
            text: 要清理的文本
            
        返回:
            清理後的文本
        """
        # 移除HTML標籤
        text = re.sub(r'<[^>]+>', '', text)
        
        # 替換HTML實體
        text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = text.replace('&quot;', '"').replace('&#39;', "'")
        
        # 統一空白符
        text = re.sub(r'\s+', ' ', text).strip()
        
        # 移除非法文件名字符
        text = re.sub(r'[\\/*?:"<>|]', '_', text)
        
        return text
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """提取URL的域名"""
        parsed_url = urlparse(url)
        return parsed_url.netloc
    
    @staticmethod
    def normalize_url(url: str, base_url: str = "") -> str:
        """標準化URL（處理相對路徑等）"""
        if not url:
            return ""
            
        url = url.strip()
        
        if url.startswith('//'):
            url = 'https:' + url
        elif not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
            
        return url
    
    @staticmethod
    def generate_filename(article: Article, suffix: str = "") -> str:
        """
        為爬取的內容生成檔案名
        
        參數:
            article: Article對象
            suffix: 檔案名後綴
            
        返回:
            檔案名
        """
        if article.id:
            base_name = article.id
        elif article.title:
            # 截取標題前50個字符作為檔案名
            base_name = TextUtils.clean_text(article.title[:50])
        else:
            parsed_url = urlparse(article.url)
            base_name = parsed_url.netloc.replace('.', '_')
        
        if article.author:
            base_name = f"{base_name}_{TextUtils.clean_text(article.author)}"
            
        if suffix:
            base_name = f"{base_name}_{suffix}"
            
        # 確保檔案名安全
        base_name = re.sub(r'\s+', '_', base_name)
        
        return f"{base_name}.txt"

class NetworkUtils:
    """網絡相關工具類"""
    
    @staticmethod
    def is_valid_content_type(headers: Dict[str, str], valid_types: List[str] = None) -> bool:
        """
        檢查HTTP響應的內容類型是否有效
        
        參數:
            headers: HTTP響應頭
            valid_types: 有效的內容類型列表，默認為["text/html", "application/json"]
            
        返回:
            是否為有效內容類型
        """
        if valid_types is None:
            valid_types = ["text/html", "application/json"]
            
        content_type = headers.get('Content-Type', '').lower()
        return any(vtype in content_type for vtype in valid_types)
    
    @staticmethod
    @contextmanager
    def timeout(seconds: int):
        """
        超時上下文管理器
        
        參數:
            seconds: 超時秒數
        """
        def signal_handler(signum, frame):
            raise TimeoutError(f"執行超時（{seconds}秒）")
            
        original_handler = signal.signal(signal.SIGALRM, signal_handler)
        signal.alarm(seconds)
        
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, original_handler)

#-------------------------------------------------------
# 用戶代理與請求頭管理
#-------------------------------------------------------

class UserAgentManager:
    """用戶代理管理器，提供隨機化的請求頭"""
    
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1"
        ]
        
        self.accept_languages = [
            "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "zh-CN,zh;q=0.9,zh-TW;q=0.8,en;q=0.7",
            "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
            "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
        ]
        
        self.lock = threading.Lock()
        self.index = 0
    
    def get_next_user_agent(self) -> str:
        """獲取下一個用戶代理（循環）"""
        with self.lock:
            agent = self.user_agents[self.index]
            self.index = (self.index + 1) % len(self.user_agents)
            return agent
    
    def get_random_headers(self) -> Dict[str, str]:
        """生成隨機化的請求頭"""
        import random
        
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": random.choice(self.accept_languages),
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": random.choice(["max-age=0", "no-cache"])
        }
        
        # 隨機添加一些額外頭部以模擬真實瀏覽器
        if random.random() < 0.3:
            headers["Referer"] = "https://www.google.com/"
        
        return headers

#-------------------------------------------------------
# 限速器
#-------------------------------------------------------

class RateLimiter:
    """限速器，實現請求頻率限制"""
    
    def __init__(self, requests_per_minute: int = 30, per_domain: bool = True):
        self.requests_per_minute = requests_per_minute
        self.per_domain = per_domain
        self.request_timestamps = {}  # 域名 -> 時間戳列表
        self.locks = {}  # 域名 -> 鎖
        self.global_lock = threading.Lock()
    
    def _get_domain_lock(self, domain: str) -> threading.Lock:
        """獲取域名對應的鎖"""
        with self.global_lock:
            if domain not in self.locks:
                self.locks[domain] = threading.Lock()
            return self.locks[domain]
    
    def _clean_old_timestamps(self, domain: str):
        """清理舊的時間戳（1分鐘前的）"""
        now = time.time()
        with self._get_domain_lock(domain):
            if domain in self.request_timestamps:
                self.request_timestamps[domain] = [
                    ts for ts in self.request_timestamps[domain] if now - ts < 60
                ]
    
    def _get_current_rate(self, domain: str) -> int:
        """獲取當前域名的每分鐘請求率"""
        self._clean_old_timestamps(domain)
        with self._get_domain_lock(domain):
            if domain not in self.request_timestamps:
                return 0
            return len(self.request_timestamps[domain])
    
    def wait(self, url: str):
        """等待適當的時間，確保不超過限流"""
        domain = TextUtils.extract_domain(url) if self.per_domain else "global"
        
        # 初始化時間戳列表
        with self.global_lock:
            if domain not in self.request_timestamps:
                self.request_timestamps[domain] = []
        
        current_rate = self._get_current_rate(domain)
        
        # 如果當前速率超過限制，需要等待
        if current_rate >= self.requests_per_minute:
            with self._get_domain_lock(domain):
                # 計算需要等待的時間
                oldest_timestamp = min(self.request_timestamps[domain])
                wait_time = max(0, 60 - (time.time() - oldest_timestamp))
                
                if wait_time > 0:
                    logger.debug(f"限速: 域名 {domain} 等待 {wait_time:.2f} 秒")
                    time.sleep(wait_time)
        
        # 記錄當前請求時間戳
        with self._get_domain_lock(domain):
            self.request_timestamps[domain].append(time.time())

#-------------------------------------------------------
# 重試機制
#-------------------------------------------------------

class RetryHandler:
    """請求重試處理器"""
    
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0, retry_on_exceptions: Tuple = None):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on_exceptions = retry_on_exceptions or (NetworkException, requests.RequestException, TimeoutError)
    
    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """
        使用重試機制執行函數
        
        參數:
            func: 要執行的函數
            args, kwargs: 函數參數
            
        返回:
            函數執行結果
            
        異常:
            如果重試後仍失敗，則拋出最後一次的異常
        """
        last_exception = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except self.retry_on_exceptions as e:
                last_exception = e
                
                if attempt < self.max_retries:
                    delay = self.backoff_factor ** (attempt - 1)
                    logger.warning(f"嘗試 {attempt}/{self.max_retries} 失敗: {str(e)}. {delay:.2f}秒後重試")
                    time.sleep(delay)
                else:
                    logger.error(f"達到最大重試次數 {self.max_retries}. 最後錯誤: {str(e)}")
        
        # 所有重試都失敗，拋出最後一次的異常
        raise last_exception

#-------------------------------------------------------
# JSON輸入處理器
#-------------------------------------------------------

class JsonInputProcessor:
    """JSON輸入檔案處理器"""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.retry_handler = RetryHandler(max_retries=3, backoff_factor=1.5)
    
    def parse(self) -> List[Dict[str, Any]]:
        """
        解析JSON輸入檔案
        
        返回:
            包含URL和元數據的字典列表
        """
        try:
            return self.retry_handler.execute_with_retry(self._parse_impl)
        except Exception as e:
            logger.error(f"解析輸入檔案時出錯: {e}")
            return []
    
    def _parse_impl(self) -> List[Dict[str, Any]]:
        """內部解析實現"""
        if not os.path.exists(self.file_path):
            raise InputOutputException(f"輸入檔案不存在: {self.file_path}")
            
        with open(self.file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON格式不完整，嘗試修復: {e}")
            try:
                # 嘗試修復不完整的JSON
                if not content.strip().startswith('{'):
                    content = '{' + content
                if not content.strip().endswith('}'):
                    content = content + '}'
                data = json.loads(content)
            except json.JSONDecodeError as e:
                raise ParseException(f"JSON解析失敗: {e}")
        
        urls = []
        # 從多種可能的數據結構中提取URL
        if isinstance(data, dict):
            # 處理包含"urls"鍵的結構
            if 'urls' in data:
                for item in data['urls']:
                    if 'url' in item:
                        urls.append({
                            'url': item['url'],
                            'title': TextUtils.clean_text(item.get('title', '')),
                            'id': item.get('id', ''),
                            'author': item.get('author', '')
                        })
            # CSDN API響應格式
            elif 'result_vos' in data:
                for item in data['result_vos']:
                    if 'url' in item:
                        urls.append({
                            'url': item['url'],
                            'title': TextUtils.clean_text(item.get('title', '')),
                            'id': item.get('id', ''),
                            'author': item.get('author', '')
                        })
            # 通用鍵值格式
            elif 'items' in data or 'results' in data or 'data' in data:
                items = data.get('items') or data.get('results') or data.get('data') or []
                for item in items:
                    if 'url' in item:
                        urls.append({
                            'url': item['url'],
                            'title': TextUtils.clean_text(item.get('title', '')),
                            'id': item.get('id', ''),
                            'author': item.get('author', '')
                        })
        # 直接URL數組
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and 'url' in item:
                    urls.append({
                        'url': item['url'],
                        'title': TextUtils.clean_text(item.get('title', '')),
                        'id': item.get('id', ''),
                        'author': item.get('author', '')
                    })
                elif isinstance(item, str) and (item.startswith('http://') or item.startswith('https://')):
                    urls.append({'url': item})
        
        logger.info(f"找到 {len(urls)} 個URL等待爬取")
        return urls

#-------------------------------------------------------
# 付費牆檢測
#-------------------------------------------------------

# 定義付費牆檢測接口協議
class PaywallDetectorProtocol(Protocol):
    """付費牆檢測器接口協議"""
    
    def detect(self, soup: BeautifulSoup, html_text: str, url: str) -> Union[Tuple[bool, int], Tuple[bool, int, Dict[str, Any]]]:
        """
        檢測頁面是否包含付費牆
        返回: (是否為付費牆, 評分) 或 (是否為付費牆, 評分, 詳情)
        """
        ...

# 實際的付費牆檢測器實現
class PaywallDetector:
    """付費牆檢測器 - 優化版"""
    
    def __init__(self):
        # 原有文本標識增強版
        self.text_indicators = [
            "subscribe to continue", "訂閱以繼續閱讀", "付費閱讀", "付費內容", 
            "premium content", "會員專享", "vip內容", "訂閱會員", "付費訂閱", 
            "登錄後才能閱讀", "登入以繼續", "此文章需要付費", "立即訂閱", 
            "會員閱讀", "請訂閱", "完整內容僅對訂閱會員開放", "加入會員",
            "subscribe now", "subscription required", "premium access",
            "members only", "become a member",
            # CSDN特有付費文本標記
            "解锁文章", "最低", "元/天", "开通VIP", "会员专享", 
            "VIP專享", "VIP专享", "會員專享", "升级VIP", "查看剩余",
            "开通会员", "付费阅读"
        ]
        
        # CSS選擇器標識增強版
        self.css_indicators = [
            "div.paywall", ".subscription-wall", ".premium-content", 
            ".member-only", "#subscribe-overlay", ".paid-content",
            ".subscription-required", ".locked-content", ".paywall-overlay", 
            ".modal-subscription", ".subscription-modal", ".premium-overlay",
            ".restricted-content", ".paid-article", ".subscriber-content",
            ".locked-article", ".premium-promo",
            # CSDN特有選擇器
            ".vip-mask", "#getVipUrl", ".openvippay", ".lock-img",
            ".hide-article-box", ".hide-article-box", "div.hide-preCode-box"
        ]
        
        # CSDN特定域名列表 - 用於降低閾值處理
        self.csdn_domains = [
            "csdn.net", "blog.csdn.net", "download.csdn.net",
            "bbs.csdn.net", "edu.csdn.net"
        ]
    
    def detect(self, soup: BeautifulSoup, html_text: str, url: str) -> Tuple[bool, int, Dict[str, Any]]:
        """
        增強的多維度付費牆檢測 (O(n) 複雜度，帶詳細分析結果)
        
        參數:
            soup: BeautifulSoup對象
            html_text: 原始HTML文本
            url: 文章URL
            
        返回:
            (是否檢測到付費牆, 檢測分數, 詳細檢測結果)
        """
        score = 0  # 初始評分
        domain = TextUtils.extract_domain(url)
        detection_details = {
            "domain_signals": [],
            "text_signals": [],
            "selector_signals": [],
            "structure_signals": []
        }
        
        # 域名特徵評估優化 (O(1))
        if any(keyword in domain.lower() for keyword in ['premium', 'vip', 'plus', 'member']):
            score += 1
            detection_details["domain_signals"].append(f"Domain contains premium keywords")
        
        # CSDN特定域名處理 - 降低閾值
        is_csdn = any(csdn_domain in domain.lower() for csdn_domain in self.csdn_domains)
        if is_csdn:
            detection_details["domain_signals"].append(f"CSDN domain detected")
        
        # 1. 深度文本分析 (改進的KMP算法實現, O(n+m))
        lower_text = html_text.lower()
        for pattern in self.text_indicators:
            pattern_lower = pattern.lower()
            if pattern_lower in lower_text:
                # 根據上下文權重調整得分
                context_weight = 1.0
                
                # 對CSDN特有付費提示給予更高權重
                if is_csdn and any(kw in pattern_lower for kw in ["解锁文章", "元/天", "开通vip"]):
                    context_weight = 2.0
                    
                score += 2 * context_weight
                detection_details["text_signals"].append(f"Paywall keyword: '{pattern}'")
        
        # 2. 選擇器高效檢測 (O(k) 其中k為選擇器數量)
        for selector in self.css_indicators:
            matching_elements = soup.select(selector)
            if matching_elements:
                # CSDN特定選擇器加權處理
                selector_weight = 2.0 if is_csdn and selector in [".vip-mask", "#getVipUrl"] else 1.0
                score += 3 * selector_weight
                detection_details["selector_signals"].append(f"Paywall selector: '{selector}' ({len(matching_elements)} elements)")
                
                # 分析元素內容以獲取更多上下文
                for elem in matching_elements[:3]:  # 限制分析的元素數量
                    elem_text = elem.get_text(strip=True)
                    if elem_text and len(elem_text) < 100:  # 只分析簡短文本
                        detection_details["selector_signals"].append(f"  - Content: '{elem_text[:50]}...'")
        
        # 3. 結構模式識別 - 特別是CSDN付費牆特定結構 (O(m))
        
        # 3.1 vip-mask結構檢測 - CSDN特有
        vip_masks = soup.select(".vip-mask")
        if vip_masks:
            score += 5  # 這是一個非常強的指標
            detection_details["structure_signals"].append(f"CSDN VIP mask structure detected")
            
            # 深度分析VIP區塊
            for mask in vip_masks:
                vip_link = mask.select_one("#getVipUrl, .openvippay")
                if vip_link:
                    link_text = vip_link.get_text(strip=True)
                    detection_details["structure_signals"].append(f"  - VIP link text: '{link_text}'")
                    if "解锁文章" in link_text or "元/天" in link_text:
                        score += 3
        
        # 3.2 通用遮罩層檢測 - 改進的DOM樹分析算法
        overlays = soup.find_all(lambda tag: 
            tag.name == "div" and 
            tag.has_attr("style") and 
            ("position: fixed" in tag.get("style", "").lower() or 
             "z-index" in tag.get("style", "").lower()))
        
        for overlay in overlays:
            overlay_text = overlay.get_text(separator=" ").lower()
            if any(keyword.lower() in overlay_text for keyword in self.text_indicators):
                score += 3
                detection_details["structure_signals"].append(f"Overlay with paywall indicators")
                # 提取部分文本作為證據
                detection_details["structure_signals"].append(f"  - Text: '{overlay_text[:100]}...'")
        
        # 4. 內容長度智能分析 - 針對CSDN特別優化
        main_content = None
        content_length = 0
        content_text = ""
        
        # 嘗試不同的內容選擇器，CSDN優先
        content_selectors = [
            "div#article_content", "div.article_content", 
            "article", "main", ".blog-content-box"
        ] if is_csdn else [
            "article", "main", "#content", ".content", ".article-content"
        ]
        
        for selector in content_selectors:
            main_content = soup.select_one(selector)
            if main_content:
                content_text = main_content.get_text(strip=True)
                content_length = len(content_text)
                break
        
        if main_content:
            detection_details["content_analysis"] = {
                "length": content_length,
                "selector": selector if 'selector' in locals() else None
            }
            
            # 短內容分析
            if content_length < 500:
                score += 1
                detection_details["content_analysis"]["signal"] = "Short content"
                
                # 如果短內容中包含付費提示，增加分數
                if any(keyword.lower() in content_text.lower() for keyword in self.text_indicators):
                    score += 3
                    detection_details["content_analysis"]["signal_detail"] = "Short content with paywall indicators"
            
            # CSDN特定模式：內容底部有隱藏層
            if is_csdn:
                hide_boxes = main_content.select(".hide-article-box, .hide-preCode-box")
                if hide_boxes:
                    score += 4
                    detection_details["content_analysis"]["hidden_content"] = True
                    detection_details["content_analysis"]["hidden_elements"] = len(hide_boxes)
        else:
            # 找不到主要內容，可能也是一種付費牆特徵
            score += 1
            detection_details["content_analysis"] = {"signal": "No main content found"}
        
        # 根據網站特性調整閾值 (O(1))
        base_threshold = 4
        threshold = 3 if is_csdn else base_threshold  # CSDN使用較低閾值
        
        # 高風險網站額外處理
        premium_domains = ["medium.com", "nytimes.com", "wsj.com", "ft.com", "economist.com"]
        if any(d in domain for d in premium_domains):
            threshold = 3
            detection_details["threshold_adjustment"] = f"Premium domain threshold: {threshold}"
        elif is_csdn:
            detection_details["threshold_adjustment"] = f"CSDN domain threshold: {threshold}"
        else:
            detection_details["threshold_adjustment"] = f"Standard threshold: {threshold}"
        
        # 最終評估結果與詳細分析
        detection_details["score"] = score
        detection_details["threshold"] = threshold
        detection_details["is_paywall"] = score >= threshold
        
        return score >= threshold, score, detection_details

#-------------------------------------------------------
# 內容提取器
#-------------------------------------------------------

class ContentExtractor(ABC):
    """內容提取器抽象基類"""
    
    @abstractmethod
    def can_handle(self, url: str, soup: BeautifulSoup) -> bool:
        """判斷是否能處理該URL"""
        pass
    
    @abstractmethod
    def extract(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """
        提取內容
        
        參數:
            url: 文章URL
            soup: BeautifulSoup對象
            html: 原始HTML
            
        返回:
            包含提取內容的字典
        """
        pass

class GenericContentExtractor(ContentExtractor):
    """通用內容提取器"""
    
    def can_handle(self, url: str, soup: BeautifulSoup) -> bool:
        """能處理所有URL（兜底提取器）"""
        return True
    
    def extract(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """提取通用網頁內容"""
        result = {
            'title': '',
            'author': '',
            'content': '',
            'publish_date': '',
            'images': []
        }
        
        # 提取標題
        title_tag = soup.find('title')
        if title_tag:
            result['title'] = title_tag.text.strip()
        
        # 嘗試不同的選擇器尋找主要內容
        content_selectors = [
            "article", "main", "#content", ".content", ".article-content", 
            ".post-content", ".entry-content", "[itemprop='articleBody']",
            ".article", ".post", ".entry", ".blog-post"
        ]
        
        main_content = None
        for selector in content_selectors:
            try:
                main_content = soup.select_one(selector)
                if main_content and len(main_content.get_text(strip=True)) > 200:
                    break
            except Exception:
                continue
        
        # 如果找不到主要內容，退而求其次使用body
        if not main_content or len(main_content.get_text(strip=True)) < 200:
            main_content = soup.body
        
        if main_content:
            # 提取正文
            paragraphs = []
            for p in main_content.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
                text = p.get_text(strip=True)
                if text and len(text) > 20:  # 忽略太短的段落
                    tag_name = p.name
                    if tag_name.startswith('h'):
                        paragraphs.append(f"\n## {text}\n")
                    else:
                        paragraphs.append(f"{text}\n\n")
            
            result['content'] = "".join(paragraphs)
            
            # 提取圖片
            for img in main_content.find_all('img'):
                img_url = img.get('src') or img.get('data-src') or img.get('data-original')
                if img_url:
                    img_alt = img.get('alt', '')
                    result['images'].append({
                        'url': TextUtils.normalize_url(img_url, url),
                        'alt': img_alt
                    })
        
        # 嘗試提取作者信息
        author_selectors = [
            "[rel='author']", ".author", ".byline", "[itemprop='author']", 
            ".post-author", ".entry-author", ".article-author"
        ]
        
        for selector in author_selectors:
            author_element = soup.select_one(selector)
            if author_element:
                result['author'] = author_element.get_text(strip=True)
                break
        
        # 嘗試提取發布日期
        date_selectors = [
            "[itemprop='datePublished']", ".published", ".post-date", 
            ".entry-date", ".article-date", "time", "[datetime]"
        ]
        
        for selector in date_selectors:
            date_element = soup.select_one(selector)
            if date_element:
                if date_element.has_attr('datetime'):
                    result['publish_date'] = date_element['datetime']
                else:
                    result['publish_date'] = date_element.get_text(strip=True)
                break
        
        return result

class CSDNContentExtractor(ContentExtractor):
    """CSDN網站專用內容提取器 - 加強版"""
    
    def can_handle(self, url: str, soup: BeautifulSoup) -> bool:
        """判斷是否為CSDN網站 (O(1))"""
        return "csdn.net" in url.lower()
    
    def _analyze_paywall_structure(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        專門分析CSDN付費牆結構 (O(k))
        返回付費牆特徵分析結果
        """
        paywall_analysis = {
            "detected": False,
            "elements": [],
            "text_fragments": []
        }
        
        # 檢查VIP遮罩
        vip_masks = soup.select(".vip-mask")
        if vip_masks:
            paywall_analysis["detected"] = True
            paywall_analysis["elements"].append(".vip-mask")
            
            for mask in vip_masks:
                text = mask.get_text(strip=True)
                paywall_analysis["text_fragments"].append(text)
        
        # 檢查隱藏內容盒子
        hide_boxes = soup.select(".hide-article-box, .hide-preCode-box")
        if hide_boxes:
            paywall_analysis["detected"] = True
            paywall_analysis["elements"].append(".hide-article-box")
            
            for box in hide_boxes:
                text = box.get_text(strip=True)
                if text:
                    paywall_analysis["text_fragments"].append(text[:100])
        
        # 檢查VIP按鈕
        vip_buttons = soup.select("#getVipUrl, .openvippay")
        if vip_buttons:
            paywall_analysis["detected"] = True
            paywall_analysis["elements"].append("#getVipUrl")
            
            for button in vip_buttons:
                text = button.get_text(strip=True)
                paywall_analysis["text_fragments"].append(text)
        
        return paywall_analysis
    
    def extract(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
        """提取CSDN文章內容 (增強版)"""
        result = {
            'title': '',
            'author': '',
            'content': '',
            'publish_date': '',
            'images': [],
            'is_paywall': False,
            'paywall_analysis': {}
        }
        
        # 1. 付費牆檢測 - 專用於CSDN的精確分析
        paywall_analysis = self._analyze_paywall_structure(soup)
        result['is_paywall'] = paywall_analysis["detected"]
        result['paywall_analysis'] = paywall_analysis
        
        # 2. 提取標題 (多級選擇器探測)
        title_selectors = [
            'h1.title-article',
            '.article-title',
            '#articleContentId',
            'h1.title',
            'h1',
            'title'
        ]
        
        for selector in title_selectors:
            title_element = soup.select_one(selector)
            if title_element:
                result['title'] = title_element.get_text(strip=True)
                break
        
        # 3. 提取作者 (多級選擇器探測)
        author_selectors = [
            '#mainBox .user-info .user-name',
            '.follow-nickName',
            '.blog-nickname',
            '.article-bar-top .follow-nickName',
            'a.follow-nickName'
        ]
        
        for selector in author_selectors:
            author_element = soup.select_one(selector)
            if author_element:
                result['author'] = author_element.get_text(strip=True)
                break
        
        # 4. 提取發布日期
        date_selectors = [
            '.time',
            '.date',
            '.article-info-box .date',
            'span[data-time], span.time'
        ]
        
        for selector in date_selectors:
            date_element = soup.select_one(selector)
            if date_element:
                result['publish_date'] = date_element.get_text(strip=True)
                break
        
        # 5. 提取文章內容 - 自適應選擇器策略
        content_container = None
        content_selectors = [
            'div#article_content',
            'div.article_content',
            'article',
            'div#content_views',
            'div.blog-content-box',
            '.markdown_views'
        ]
        
        # 選擇最佳內容容器 (基於文本長度啟發式)
        best_container = None
        max_content_length = 0
        
        for selector in content_selectors:
            container = soup.select_one(selector)
            if container:
                text_length = len(container.get_text(strip=True))
                if text_length > max_content_length:
                    max_content_length = text_length
                    best_container = container
        
        content_container = best_container
        
        # 完全移除所有不需要的UI元素，確保它們的內容（包括圖片）都不會被處理
        # 1. 移除文章信息框
        for article_info_box in soup.select('.article-info-box'):
            article_info_box.decompose()  # 從DOM中完全移除整個元素
            
        # 2. 移除專欄廣告
        for column_advert in soup.select('#blogColumnPayAdvert'):
            column_advert.decompose()  # 從DOM中完全移除整個元素
        
        if content_container:
            # 識別並處理付費牆限制的內容
            if result['is_paywall']:
                # 從容器中刪除付費遮罩元素進行內容提取
                for mask in content_container.select(".vip-mask, .hide-article-box"):
                    mask.decompose()
            
            # 高級內容處理
            self._process_content(content_container, result)
            
            # 提取圖片 (帶高級屬性分析)
            for img in content_container.find_all('img'):
                img_url = img.get('src') or img.get('data-src') or img.get('data-original')
                if img_url:
                    # 創建帶有元數據的圖像記錄
                    img_data = {
                        'url': TextUtils.normalize_url(img_url, url),
                        'alt': img.get('alt', ''),
                        'width': img.get('width', ''),
                        'height': img.get('height', ''),
                        'is_latex': 'mathjax' in img.get('class', [])
                    }
                    result['images'].append(img_data)
        
        return result
    
    def _process_content(self, container, result):
        """
        高級內容處理邏輯 (分層解析算法)
        
        優化點:
        1. 代碼塊語言識別和保留
        2. 數學公式特殊處理
        3. 層次標題結構保留
        4. 表格內容的結構化提取
        """
        # 處理代碼塊
        for pre in container.find_all('pre'):
            code = pre.find('code')
            if code:
                # 語言識別和保留
                language = ''
                if code.has_attr('class'):
                    for cls in code.get('class', []):
                        if cls.startswith('language-') or cls.startswith('lang-'):
                            language = cls.replace('language-', '').replace('lang-', '')
                
                code_text = code.get_text(strip=True)
                pre.replace_with(f"\n```{language}\n{code_text}\n```\n")
        
        # 提取結構化內容
        content_parts = []
        for element in container.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'pre', 'ol', 'ul', 'table']):
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                heading_text = element.get_text(strip=True)
                heading_level = int(element.name[1])
                content_parts.append(f"\n{'#' * heading_level} {heading_text}\n")
            elif element.name == 'pre':
                # 預設<pre>標籤已經在之前的處理中轉換為Markdown代碼塊
                content_parts.append(str(element))
            elif element.name in ['ol', 'ul']:
                list_items = []
                for i, li in enumerate(element.find_all('li')):
                    marker = f"{i+1}." if element.name == 'ol' else "-"
                    list_items.append(f"{marker} {li.get_text(strip=True)}")
                content_parts.append("\n" + "\n".join(list_items) + "\n")
            elif element.name == 'table':
                # 表格轉換為Markdown
                table_md = self._convert_table_to_markdown(element)
                content_parts.append(f"\n{table_md}\n")
            else:
                # 普通段落
                text = element.get_text(strip=True)
                if text:
                    # 檢查是否為數學公式段落
                    if element.find('span', class_='katex') or element.find('span', class_='MathJax'):
                        # 保留數學公式標記
                        content_parts.append(f"$$\n{text}\n$$\n\n")
                    else:
                        content_parts.append(f"{text}\n\n")
        
        result['content'] = "".join(content_parts)
    
    def _convert_table_to_markdown(self, table_element):
        """將HTML表格轉換為Markdown格式 (O(r*c))"""
        rows = []
        
        # 提取表頭
        headers = []
        header_row = table_element.find('thead')
        if header_row:
            for th in header_row.find_all(['th']):
                headers.append(th.get_text(strip=True) or ' ')
        
        # 如果沒有明確的表頭，使用第一行作為表頭
        if not headers and table_element.find('tr'):
            first_row = table_element.find('tr')
            for cell in first_row.find_all(['th', 'td']):
                headers.append(cell.get_text(strip=True) or ' ')
        
        # 確保至少有一個列
        if not headers:
            headers = [' ']
        
        # 添加表頭行
        rows.append('| ' + ' | '.join(headers) + ' |')
        
        # 添加分隔行
        rows.append('| ' + ' | '.join(['---'] * len(headers)) + ' |')
        
        # 提取表體
        for tr in table_element.find_all('tr')[1:] if headers else table_element.find_all('tr'):
            cells = []
            for cell in tr.find_all(['td', 'th']):
                cells.append(cell.get_text(strip=True) or ' ')
            
            # 確保單元格數量與表頭一致
            while len(cells) < len(headers):
                cells.append(' ')
            
            # 僅使用與表頭數量相同的單元格
            cells = cells[:len(headers)]
            
            rows.append('| ' + ' | '.join(cells) + ' |')
        
        return '\n'.join(rows)

#-------------------------------------------------------
# 內容提取器工廠
#-------------------------------------------------------

class ContentExtractorFactory:
    """內容提取器工廠，根據URL選擇合適的提取器"""
    
    def __init__(self):
        self.extractors = [
            CSDNContentExtractor(),
            # 其他特定網站的提取器可以在這裡添加
            GenericContentExtractor()  # 通用提取器應該放在最後
        ]
    
    def get_extractor(self, url: str, soup: BeautifulSoup) -> ContentExtractor:
        """獲取適合處理給定URL的提取器"""
        for extractor in self.extractors:
            if extractor.can_handle(url, soup):
                return extractor
        
        # 理論上永遠不會到達這裡，因為GenericContentExtractor可以處理所有URL
        return GenericContentExtractor()

#-------------------------------------------------------
# 圖片處理
#-------------------------------------------------------

class ImageDownloader:
    """圖片下載器"""
    
    def __init__(self, config: ScrapingConfig):
        self.config = config
        self.user_agent_manager = UserAgentManager()
        self.retry_handler = RetryHandler(
            max_retries=config.max_retries,
            backoff_factor=config.backoff_factor
        )
        self.rate_limiter = RateLimiter(requests_per_minute=20)
    
    def download(self, img_url: str, base_url: str, image_dir: str, article_id: str) -> Tuple[bool, Optional[str], str]:
        """
        下載圖片並儲存到指定目錄
        
        參數:
            img_url: 圖片URL
            base_url: 基礎URL以處理相對路徑
            image_dir: 圖片儲存目錄
            article_id: 文章標識符
            
        返回:
            (成功與否, 本地圖片路徑, 圖片URL)
        """
        # 標準化URL
        img_url = TextUtils.normalize_url(img_url, base_url)
        
        # 如果是數據URL或空URL，跳過
        if not img_url or img_url.startswith('data:'):
            return False, None, img_url
            
        try:
            # 生成圖片檔案名
            url_hash = hashlib.md5(img_url.encode()).hexdigest()
            file_ext = os.path.splitext(urlparse(img_url).path)[1]
            if not file_ext or len(file_ext) > 5:
                file_ext = '.jpg'
            img_filename = f"{article_id}_{url_hash}{file_ext}"
            img_path = os.path.join(image_dir, img_filename)
            
            # 檢查圖片是否已下載
            if os.path.exists(img_path) and os.path.getsize(img_path) > 0:
                logger.debug(f"圖片已存在，跳過下載: {img_url}")
                return True, img_path, img_url
            
            # 限速控制
            self.rate_limiter.wait(img_url)
            
            # 使用重試機制下載圖片
            return self.retry_handler.execute_with_retry(self._download_impl, img_url, img_path)
                
        except Exception as e:
            logger.error(f"下載圖片 {img_url} 失敗: {e}")
            return False, None, img_url
    
    def _download_impl(self, img_url: str, img_path: str) -> Tuple[bool, str, str]:
        """實際的下載實現（用於重試機制）"""
        headers = self.user_agent_manager.get_random_headers()
        
        response = requests.get(
            img_url, 
            headers=headers, 
            timeout=self.config.image_download_timeout, 
            stream=True,
            verify=self.config.verify_ssl
        )
        response.raise_for_status()
        
        # 檢查是否為圖片
        content_type = response.headers.get('Content-Type', '')
        if not content_type.startswith(('image/', 'application/octet-stream')):
            logger.warning(f"跳過非圖片URL: {img_url} (Content-Type: {content_type})")
            return False, None, img_url
        
        # 創建目錄（如果不存在）
        os.makedirs(os.path.dirname(img_path), exist_ok=True)
        
        # 保存圖片
        with open(img_path, 'wb') as f:
            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, f)
        
        # 驗證圖片大小
        if os.path.getsize(img_path) < 100:  # 太小可能不是有效圖片
            os.remove(img_path)
            return False, None, img_url
            
        logger.debug(f"已下載圖片: {img_url} -> {img_path}")
        return True, img_path, img_url

#-------------------------------------------------------
# 主要爬蟲類
#-------------------------------------------------------

class WebScraper:
    """網頁爬蟲主類，協調整個爬取過程"""
    
    def __init__(self, config: ScrapingConfig):
        """
        初始化網頁爬蟲與配置參數
        
        參數:
            config: 爬蟲配置參數
        """
        self.config = config
        
        # 確保輸出目錄存在
        os.makedirs(config.output_dir, exist_ok=True)
        
        # 創建圖片目錄
        self.image_dir = os.path.join(config.output_dir, 'images')
        os.makedirs(self.image_dir, exist_ok=True)
        
        # 初始化組件
        self.user_agent_manager = UserAgentManager()
        self.rate_limiter = RateLimiter(requests_per_minute=30)
        self.retry_handler = RetryHandler(
            max_retries=config.max_retries, 
            backoff_factor=config.backoff_factor
        )
        self.paywall_detector = PaywallDetector()
        self.image_downloader = ImageDownloader(config)
        self.content_extractor_factory = ContentExtractorFactory()
        
        # 初始化統計數據
        self.stats = ScrapingStats()
        self.stats.start_time = time.time()
        
        logger.info(f"初始化爬蟲 - 輸入文件: {config.input_file}, 輸出目錄: {config.output_dir}")
    
    def scrape_url(self, url_data: Dict[str, Any]) -> Optional[Article]:
        """
        爬取單個URL並處理內容
        
        參數:
            url_data: 包含URL和元數據的字典
            
        返回:
            處理後的Article對象，失敗時返回None
        """
        url = url_data.get('url', '')
        if not url:
            logger.error("遇到空URL，跳過")
            return None
            
        # 創建文章對象
        article = Article(
            url=url,
            title=url_data.get('title', ''),
            id=url_data.get('id', ''),
            author=url_data.get('author', '')
        )
        
        try:
            # 更新域名統計
            self.stats.update_domain_stats(url)
            
            # 應用速率限制
            self.rate_limiter.wait(url)
            
            # 記錄開始時間
            start_time = time.time()
            
            # 使用重試機制請求頁面
            html, response = self.retry_handler.execute_with_retry(
                self._request_page, url
            )
            
            # 計算下載時間
            article.download_time = time.time() - start_time
            
            # 解析HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # 檢測付費牆
            is_paywall, paywall_score, paywall_details = self.paywall_detector.detect(
                soup, html, url
            )
            
            article.is_paywall = is_paywall
            article.paywall_score = paywall_score
            article.paywall_details = paywall_details
            
            # 如果付費牆得分超過閾值，跳過內容提取
            if is_paywall and paywall_score >= self.config.paywall_threshold:
                logger.warning(f"檢測到付費牆 (得分: {paywall_score})，跳過內容提取: {url}")
                self.stats.paywall_count += 1
                return article
                
            # 根據URL選擇合適的內容提取器
            extractor = self.content_extractor_factory.get_extractor(url, soup)
            
            # 提取內容
            extracted_data = extractor.extract(url, soup, html)
            
            # 更新文章對象
            if not article.title and extracted_data.get('title'):
                article.title = extracted_data.get('title')
            if not article.author and extracted_data.get('author'):
                article.author = extracted_data.get('author')
            article.content = extracted_data.get('content', '')
            article.html = html
            article.publish_date = extracted_data.get('publish_date', '')
            article.text_length = len(article.content)
            
            # 檢查內容長度
            if article.text_length < self.config.min_content_length and not article.is_paywall:
                logger.warning(f"內容長度過短 ({article.text_length} 字符)，可能提取失敗: {url}")
            
            # 下載圖片（如果配置允許）
            if self.config.download_images and extracted_data.get('images'):
                self._download_article_images(article, extracted_data.get('images', []))
            
            # 更新統計
            self.stats.successful_urls += 1
            if article.is_paywall:
                self.stats.paywall_count += 1
            
            logger.info(f"成功爬取 URL: {url} (標題: {article.title[:30]}..., 長度: {article.text_length})")
            return article
            
        except Exception as e:
            self.stats.failed_urls += 1
            self.stats.update_error_stats(type(e).__name__)
                
            error_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"爬取URL {url} 時出錯: {error_msg}")
                
            # 保存錯誤信息到文章對象
            article.error = error_msg
            return article
            
        finally:
            # 強制延遲，確保不會太快請求下一個URL
            time.sleep(self.config.delay)
    
    def _request_page(self, url: str) -> Tuple[str, requests.Response]:
        """
        請求頁面內容
        
        參數:
            url: 頁面URL
            
        返回:
            (HTML內容, Response對象)
        """
        headers = self.user_agent_manager.get_random_headers()
        
        response = requests.get(
            url, 
            headers=headers, 
            timeout=self.config.timeout,
            verify=self.config.verify_ssl,
            proxies={'http': self.config.proxy, 'https': self.config.proxy} if self.config.proxy else None
        )
        response.raise_for_status()
        
        # 自動檢測編碼
        if response.encoding == 'ISO-8859-1':
            response.encoding = response.apparent_encoding
        
        return response.text, response
    
    def _download_article_images(self, article: Article, image_data: List[Dict[str, str]]):
        """下載文章中的圖片"""
        if not image_data:
            return
            
        self.stats.total_images += len(image_data)
        
        # 使用線程池並發下載圖片
        with ThreadPoolExecutor(max_workers=self.config.max_image_workers) as executor:
            futures = []
            for img_info in image_data:
                img_url = img_info['url']
                futures.append(
                    executor.submit(
                        self.image_downloader.download, 
                        img_url, 
                        article.url, 
                        self.image_dir, 
                        article.id
                    )
                )
            
            # 收集下載結果
            for future in as_completed(futures):
                try:
                    success, img_path, img_url = future.result()
                    if success and img_path:
                        rel_path = os.path.relpath(img_path, self.config.output_dir)
                        article.images.append({
                            'url': img_url,
                            'path': rel_path
                        })
                        self.stats.successful_images += 1
                    else:
                        self.stats.failed_images += 1
                except Exception as e:
                    self.stats.failed_images += 1
                    logger.error(f"處理圖片下載結果時出錯: {e}")
    
    def save_article(self, article: Article) -> Optional[str]:
        """
        將爬取的文章保存到文件
        
        參數:
            article: 文章對象
            
        返回:
            保存的文件路徑，失敗則返回None
        """
        try:
            # 如果是因為付費牆分數過高而跳過的文章，使用特殊標記
            paywall_suffix = ""
            if article.is_paywall:
                if article.paywall_score >= self.config.paywall_threshold:
                    paywall_suffix = "付費內容_未抓取"
                else:
                    paywall_suffix = "付費內容"
            
            # 生成文件名
            filename = TextUtils.generate_filename(
                article, 
                paywall_suffix
            )
            filepath = os.path.join(self.config.output_dir, filename)
            
            # 構建文章內容
            content = []
            content.append(f"標題: {article.title}\n")
            content.append(f"URL: {article.url}\n")
            content.append(f"作者: {article.author}\n")
            content.append(f"ID: {article.id}\n")
            content.append(f"發布日期: {article.publish_date}\n")
            
            if article.is_paywall:
                content.append(f"⚠️ 警告: 此文章可能需要付費訂閱才能完整閱讀 ⚠️\n")
                if article.paywall_score > 0:
                    content.append(f"付費牆檢測評分: {article.paywall_score}\n")
                    if article.paywall_score >= self.config.paywall_threshold:
                        content.append(f"⚠️ 付費牆分數超過閾值 ({self.config.paywall_threshold})，已跳過內容抓取 ⚠️\n")
            
            content.append("-" * 80 + "\n\n")
            
            # 添加主要內容
            if article.error:
                content.append(f"爬取失敗: {article.error}\n\n")
            else:
                content.append(article.content)
            
            # 添加圖片信息
            if article.images:
                content.append("\n\n" + "-" * 80 + "\n")
                content.append(f"下載的圖片 ({len(article.images)}):\n")
                for i, img in enumerate(article.images, 1):
                    content.append(f"{i}. {img['path']} (源自: {img['url']})\n")
            
            # 寫入文件
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("".join(content))
            
            logger.info(f"內容已保存到 {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"保存文章時出錯: {e}")
            return None
    
    def run(self) -> List[str]:
        """
        執行爬蟲，處理所有URL
        
        返回:
            保存的文件路徑列表
        """
        logger.info("開始執行網頁爬蟲")
        
        # 解析輸入文件
        input_processor = JsonInputProcessor(self.config.input_file)
        url_data_list = input_processor.parse()
        
        if not url_data_list:
            logger.warning("沒有找到要爬取的URL")
            return []
        
        self.stats.total_urls = len(url_data_list)
        logger.info(f"準備爬取 {self.stats.total_urls} 個URL")
        
        # 保存的文件列表
        saved_files = []
        
        # 使用線程池並發爬取
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # 提交所有任務
            future_to_url = {
                executor.submit(self.scrape_url, url_data): url_data 
                for url_data in url_data_list
            }
            
            # 處理結果
            for future in as_completed(future_to_url):
                url_data = future_to_url[future]
                try:
                    article = future.result()
                    if article:
                        filepath = self.save_article(article)
                        if filepath:
                            saved_files.append(filepath)
                except Exception as e:
                    logger.error(f"處理 {url_data['url']} 的結果時出錯: {e}")
        
        # 更新統計
        self.stats.end_time = time.time()
        
        # 輸出統計信息
        self._print_stats()
        
        # 保存統計數據
        self._save_stats()
        
        return saved_files
    
    def _print_stats(self):
        """輸出爬蟲統計信息"""
        duration = self.stats.calculate_duration()
        success_rate = self.stats.calculate_success_rate()
        image_success_rate = self.stats.calculate_image_success_rate()
        
        logger.info("=" * 60)
        logger.info("爬蟲執行統計:")
        logger.info(f"總URL數: {self.stats.total_urls}")
        logger.info(f"成功URL數: {self.stats.successful_urls} ({success_rate:.1f}%)")
        logger.info(f"失敗URL數: {self.stats.failed_urls}")
        logger.info(f"付費牆數: {self.stats.paywall_count}")
        logger.info(f"總圖片數: {self.stats.total_images}")
        logger.info(f"下載成功圖片數: {self.stats.successful_images} ({image_success_rate:.1f}%)")
        logger.info(f"執行時間: {duration:.1f} 秒")
        logger.info("=" * 60)
    
    def _save_stats(self):
        """保存爬蟲統計數據到JSON文件"""
        stats_file = os.path.join(self.config.output_dir, "scraping_stats.json")
        
        stats_data = {
            "total_urls": self.stats.total_urls,
            "successful_urls": self.stats.successful_urls,
            "failed_urls": self.stats.failed_urls,
            "success_rate": self.stats.calculate_success_rate(),
            "paywall_count": self.stats.paywall_count,
            "execution_time_seconds": self.stats.calculate_duration(),
            "image_stats": {
                "total": self.stats.total_images,
                "successful": self.stats.successful_images,
                "failed": self.stats.failed_images,
                "success_rate": self.stats.calculate_image_success_rate()
            },
            "domain_stats": self.stats.domains,
            "error_stats": self.stats.errors,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)
            logger.info(f"統計數據已保存到 {stats_file}")
        except Exception as e:
            logger.error(f"保存統計數據時出錯: {e}")

#-------------------------------------------------------
# 主函數與命令行介面
#-------------------------------------------------------

def parse_arguments():
    """解析命令行參數"""
    import argparse
    
    parser = argparse.ArgumentParser(description="JSON URL爬蟲工具")
    parser.add_argument("--input", default="./debug_output/filtered_api_response_0.json", help="包含URL的JSON輸入文件")
    parser.add_argument("--output", default="./debug_output/scraped_articles", help="爬取內容的輸出目錄")
    parser.add_argument("--workers", type=int, default=3, help="最大並行工作數")
    parser.add_argument("--delay", type=float, default=2.0, help="請求間的延遲秒數")
    parser.add_argument("--timeout", type=int, default=30, help="請求超時秒數")
    parser.add_argument("--retries", type=int, default=3, help="請求失敗時的最大重試次數")
    parser.add_argument("--no-images", action="store_true", help="不下載圖片")
    parser.add_argument("--images", action="store_true", help="下載圖片 (優先於 --no-images)")
    parser.add_argument("--proxy", help="使用代理伺服器 (格式: http://user:pass@host:port)")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="日誌級別")
    parser.add_argument("--paywall-threshold", type=int, default=30, help="付費牆閾值，超過此值不抓取內容")
    
    return parser.parse_args()

# 修改主函數，處理新增的命令行參數
def main():
    """主函數"""
    # 解析命令行參數
    args = parse_arguments()
    
    # 設置日誌級別
    log_level = getattr(logging, args.log_level)
    for handler in logger.handlers:
        handler.setLevel(log_level)
    
    # 處理圖片下載設置 (--images 優先於 --no-images)
    download_images = True
    if args.no_images and not args.images:
        download_images = False
    if args.images:
        download_images = True
    
    # 創建配置
    config = ScrapingConfig(
        input_file=args.input,
        output_dir=args.output,
        max_workers=args.workers,
        delay=args.delay,
        download_images=download_images,
        timeout=args.timeout,
        max_retries=args.retries,
        proxy=args.proxy,
        paywall_threshold=args.paywall_threshold
    )
    
    # 創建並運行爬蟲
    scraper = WebScraper(config)
    saved_files = scraper.run()
    
    # 輸出結果摘要
    print(f"\n爬取完成！")
    print(f"總共保存的檔案數: {len(saved_files)}")
    if saved_files:
        print(f"檔案保存在: {os.path.abspath(config.output_dir)}")
        
        # 分析不同類型的文件
        paywall_files = [f for f in saved_files if "_付費內容" in os.path.basename(f)]
        skipped_paywall_files = [f for f in saved_files if "_付費內容_未抓取" in os.path.basename(f)]
        
        if skipped_paywall_files:
            print(f"檢測到 {len(skipped_paywall_files)} 個高度付費文章 (評分 >= {config.paywall_threshold})，未抓取內容。")
        
        if paywall_files and len(paywall_files) > len(skipped_paywall_files):
            normal_paywall_count = len(paywall_files) - len(skipped_paywall_files)
            print(f"檢測到 {normal_paywall_count} 個付費文章 (評分 < {config.paywall_threshold})，已抓取內容。")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n程序被用戶中斷")
    except Exception as e:
        logger.error(f"程序執行時發生未捕獲的異常: {e}", exc_info=True)
        print(f"\n程序執行出錯: {e}")
        print("詳細錯誤信息已記錄到日誌")
    finally:
        # 確保關閉所有日誌處理器
        logging.shutdown()
