import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import json
import os
import argparse
import math
from urllib.parse import urljoin, urlparse, quote
from typing import Dict, List, Optional, Union, Tuple, Any
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum, auto
import hashlib
from abc import ABC, abstractmethod

# 配置日誌記錄
def setup_logger(name="csdn_scraper", level=logging.DEBUG, log_file="csdn_debug.log"):
    """設置並返回配置好的logger實例"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # 防止重複添加handler
    if not logger.handlers:
        # 文件處理器
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        
        # 控制台處理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        
        # 格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # 添加處理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# 初始化主logger
logger = setup_logger()

# 檢查Selenium可用性
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium未安裝，將無法處理動態內容。建議執行: pip install selenium webdriver-manager")

#-------------------------------------------------------
# 數據模型定義
#-------------------------------------------------------

@dataclass
class Article:
    """文章數據模型"""
    index: int
    title: str = ""
    url: str = ""
    author: str = ""
    content_preview: str = ""
    publish_date: str = ""
    html: str = ""
    links: List[Dict[str, str]] = None
    
    def __post_init__(self):
        if self.links is None:
            self.links = []

class SearchMethod(Enum):
    """搜索方法枚舉"""
    STATIC = auto()
    API = auto()
    DYNAMIC = auto()
    ALL = auto()

@dataclass
class SearchResult:
    """搜索結果數據模型"""
    success: bool
    method: SearchMethod
    articles: List[Article] = None
    total_count: int = 0
    error_message: str = ""
    response_time: float = 0.0
    
    def __post_init__(self):
        if self.articles is None:
            self.articles = []

#-------------------------------------------------------
# 用戶代理與請求頭處理
#-------------------------------------------------------

class UserAgentManager:
    """用戶代理管理器，提供隨機化的請求頭"""
    
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:92.0) Gecko/20100101 Firefox/92.0",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1"
        ]
        
        self.accept_languages = [
            "zh-CN,zh;q=0.9,en;q=0.8",
            "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
            "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "zh-CN,zh;q=0.9,zh-TW;q=0.8,en;q=0.7",
            "en-GB,en;q=0.9,en-US;q=0.8"
        ]
    
    def get_random_headers(self, for_api: bool = False) -> Dict[str, str]:
        """生成隨機化的請求頭，降低被拦截風險"""
        headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept-Language": random.choice(self.accept_languages), 
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": random.choice(["keep-alive", "close"]),
            "Cache-Control": random.choice(["max-age=0", "no-cache"]),
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1"
        }
        
        if for_api:
            headers.update({
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/json"
            })
        else:
            headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            headers["Upgrade-Insecure-Requests"] = "1"
        
        return headers

#-------------------------------------------------------
# 請求管理與限速機制
#-------------------------------------------------------

class RateLimiter:
    """限速器，實現自適應的延遲和限流策略"""
    
    def __init__(self, requests_per_minute: int = 30, backoff_factor: float = 2.0):
        self.requests_per_minute = requests_per_minute
        self.backoff_factor = backoff_factor
        self.request_count = 0
        self.request_timestamps = []
        self.lock = threading.Lock()
    
    def _clean_old_timestamps(self):
        """清理舊的時間戳（1分鐘前的）"""
        now = time.time()
        self.request_timestamps = [ts for ts in self.request_timestamps if now - ts < 60]
    
    def _get_current_rate(self) -> int:
        """獲取當前每分鐘的請求率"""
        self._clean_old_timestamps()
        return len(self.request_timestamps)
    
    def _calculate_delay(self) -> float:
        """計算應該延遲的時間"""
        self.request_count += 1
        current_rate = self._get_current_rate()
        
        # 基礎延遲
        base_delay = 60.0 / self.requests_per_minute
        
        # 如果當前速率超過或接近限制，增加延遲
        if current_rate >= self.requests_per_minute:
            base_delay *= self.backoff_factor
        
        # 為每個請求添加一些隨機延遲(10%-150%)
        jitter = random.uniform(0.1, 1.5)
        delay = base_delay * jitter
        
        # 針對超過閾值的請求計數應用指數退避
        if self.request_count > 10:
            delay *= math.log(self.request_count, 2)
        
        return delay
    
    def wait(self):
        """等待適當的時間，確保不超過限流"""
        with self.lock:
            delay = self._calculate_delay()
            logger.debug(f"等待 {delay:.2f} 秒")
            time.sleep(delay)
            
            # 記錄當前時間戳
            self.request_timestamps.append(time.time())

#-------------------------------------------------------
# WebDriver管理
#-------------------------------------------------------

class WebDriverManager:
    """WebDriver管理器，負責創建和管理Selenium WebDriver實例"""
    
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.user_agent_manager = UserAgentManager()
    
    def create_driver(self) -> Optional[webdriver.Chrome]:
        """創建並配置WebDriver實例"""
        if not SELENIUM_AVAILABLE:
            logger.error("無法創建WebDriver，因為Selenium未安裝")
            return None
        
        try:
            options = Options()
            if self.headless:
                options.add_argument('--headless')
            
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument(f'user-agent={self.user_agent_manager.get_random_headers()["User-Agent"]}')
            options.add_argument('--disable-blink-features=AutomationControlled')  # 隱藏自動化特征
            
            # 添加實驗性選項，進一步隱藏自動化特征
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # 創建WebDriver
            driver = webdriver.Chrome(options=options)
            
            # 執行JavaScript修改navigator屬性，隱藏webdriver標誌
            driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
            
            return driver
        except Exception as e:
            logger.error(f"創建WebDriver時出錯: {e}")
            return None

class WebDriverContextManager:
    """WebDriver上下文管理器，確保正確釋放資源"""
    
    def __init__(self, manager: WebDriverManager):
        self.manager = manager
        self.driver = None
    
    def __enter__(self):
        self.driver = self.manager.create_driver()
        return self.driver
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"關閉WebDriver時出錯: {e}")

#-------------------------------------------------------
# 搜索策略接口
#-------------------------------------------------------

class SearchStrategy(ABC):
    """搜索策略接口，定義搜索行為的抽象基類"""
    
    def __init__(self, output_dir: str = "debug_output"):
        self.output_dir = output_dir
        # 確保輸出目錄存在
        os.makedirs(output_dir, exist_ok=True)
        
    @abstractmethod
    def search(self, query: str, start_index: int = 0, article_count: Optional[int] = None) -> SearchResult:
        """執行搜索操作"""
        pass

#-------------------------------------------------------
# API搜索策略實現
#-------------------------------------------------------

class ApiSearchStrategy(SearchStrategy):
    """通過API進行CSDN搜索的策略實現"""
    
    def __init__(self, output_dir: str = "debug_output"):
        super().__init__(output_dir)
        self.user_agent_manager = UserAgentManager()
        self.rate_limiter = RateLimiter()
        
    def search(self, query: str, start_index: int = 0, article_count: Optional[int] = None) -> SearchResult:
        """
        通過CSDN搜索API執行搜索
        
        參數:
            query: 搜索關鍵詞
            start_index: 開始的文章索引（從0開始）
            article_count: 要獲取的文章數量
        """
        logger.info(f"開始使用API方式搜索CSDN: {query} [文章範圍: {start_index} 起始, 數量 {article_count if article_count else '全部'}]")
        
        # 構建請求頭
        headers = self.user_agent_manager.get_random_headers(for_api=True)
        
        # 應用限速
        self.rate_limiter.wait()
        
        start_time = time.time()
        try:
            # 正確編碼查詢字符串
            encoded_query = quote(query)
            
            # 計算頁碼 (假設每頁20篇文章)
            page = start_index // 20 + 1
            
            # 嘗試多個可能的API端點
            api_endpoints = [
                f"https://so.csdn.net/api/v3/search?q={encoded_query}&t=blog&p={page}&s=0&tm=0",
                f"https://so.csdn.net/so/search/api/searchV2?q={encoded_query}&t=blog&p={page}&s=0&tm=0",
                f"https://so.csdn.net/so/search/s.do?q={encoded_query}&t=blog&p={page}&s=0&tm=0&format=json"
            ]
            
            for endpoint in api_endpoints:
                logger.info(f"嘗試API端點: {endpoint}")
                
                # 發送請求
                response = requests.get(endpoint, headers=headers, timeout=30)
                
                # 嘗試解析JSON響應
                try:
                    json_data = response.json()
                    
                    # 保存原始API響應
                    api_path = os.path.join(self.output_dir, f"api_response_{api_endpoints.index(endpoint)}.json")
                    with open(api_path, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=2)
                    logger.info(f"已保存API響應到 {api_path}")
                    
                    # 如果響應中包含數據，處理並返回
                    if isinstance(json_data, dict):
                        # 嘗試不同的數據字段名
                        for data_field in ['result_vos', 'data', 'results', 'items']:
                            if data_field in json_data:
                                # 找到數據字段
                                all_items = json_data[data_field]
                                total_count = len(all_items)
                                
                                # 計算在頁內的偏移 (基於0的索引)
                                in_page_offset = start_index % 20
                                
                                # 應用範圍篩選
                                end_offset = total_count if article_count is None else min(in_page_offset + article_count, total_count)
                                filtered_items = all_items[in_page_offset:end_offset]
                                
                                # 創建筛選後的響應
                                filtered_response = json_data.copy()
                                filtered_response[data_field] = filtered_items
                                
                                # 保存筛選後的API響應
                                filtered_api_path = os.path.join(self.output_dir, f"filtered_api_response_{api_endpoints.index(endpoint)}.json")
                                with open(filtered_api_path, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_response, f, ensure_ascii=False, indent=2)
                                logger.info(f"已保存筩選後的API響應到 {filtered_api_path}，包含 {len(filtered_items)} 篇文章")
                                
                                # 轉換為Article對象
                                articles = []
                                for i, item in enumerate(filtered_items):
                                    article = Article(
                                        index=start_index + i,
                                        title=item.get('title', ''),
                                        url=item.get('url', ''),
                                        author=item.get('author', ''),
                                        content_preview=item.get('description', ''),
                                        publish_date=item.get('created_at', '')
                                    )
                                    articles.append(article)
                                
                                # 返回搜索結果
                                result = SearchResult(
                                    success=True,
                                    method=SearchMethod.API,
                                    articles=articles,
                                    total_count=total_count,
                                    response_time=time.time() - start_time
                                )
                                return result
                    
                    logger.warning(f"API響應中未找到預期的數據字段")
                    
                except json.JSONDecodeError:
                    logger.info(f"端點不返回JSON: {endpoint}")
                    continue
            
            # 如果所有API端點都失敗
            logger.warning("所有API端點嘗試失敗")
            return SearchResult(
                success=False,
                method=SearchMethod.API, 
                error_message="所有API端點嘗試失敗",
                response_time=time.time() - start_time
            )
                
        except Exception as e:
            logger.error(f"API搜索過程中出錯: {e}", exc_info=True)
            return SearchResult(
                success=False,
                method=SearchMethod.API, 
                error_message=f"搜索過程中出錯: {str(e)}",
                response_time=time.time() - start_time
            )

#-------------------------------------------------------
# 動態搜索策略實現
#-------------------------------------------------------

class DynamicSearchStrategy(SearchStrategy):
    """使用Selenium動態渲染進行CSDN搜索的策略實現"""
    
    def __init__(self, output_dir: str = "debug_output", headless: bool = True):
        super().__init__(output_dir)
        self.webdriver_manager = WebDriverManager(headless=headless)
        self.rate_limiter = RateLimiter(requests_per_minute=10)  # 降低Selenium請求頻率
    
    def search(self, query: str, start_index: int = 0, article_count: Optional[int] = None) -> SearchResult:
        """
        使用Selenium動態渲染CSDN搜索結果，支持無限滾動加載更多文章
        
        參數:
            query: 搜索關鍵詞
            start_index: 開始的文章索引（從0開始）
            article_count: 要獲取的文章數量
        """
        if not SELENIUM_AVAILABLE:
            logger.error("無法使用動態請求方式，因為Selenium未安裝")
            return SearchResult(
                success=False,
                method=SearchMethod.DYNAMIC,
                error_message="Selenium未安裝，無法使用動態請求方式"
            )
        
        logger.info(f"開始使用動態請求方式搜索CSDN: {query} [文章範圍: {start_index} 起始, 數量 {article_count if article_count else '全部'}]")
        
        # 應用限速
        self.rate_limiter.wait()
        
        start_time = time.time()
        
        with WebDriverContextManager(self.webdriver_manager) as driver:
            if not driver:
                return SearchResult(
                    success=False,
                    method=SearchMethod.DYNAMIC,
                    error_message="無法創建WebDriver實例",
                    response_time=time.time() - start_time
                )
            
            try:
                # 正確編碼查詢字符串
                encoded_query = quote(query)
                search_url = f"https://so.csdn.net/so/search/s.do?q={encoded_query}&t=blog"
                
                logger.info(f"使用Selenium請求URL: {search_url}")
                
                # 打開URL
                driver.get(search_url)
                
                # 嘗試多種可能的選擇器
                selectors = [
                    ".search-list-container .search-list-item",
                    ".search-list .search-item",
                    "div.result-item",
                    ".common-search-item",
                    ".list-item"
                ]
                
                # 找到有效的選擇器
                valid_selector = None
                for selector in selectors:
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        )
                        valid_selector = selector
                        logger.info(f"找到有效選擇器：{valid_selector}")
                        break
                    except TimeoutException:
                        continue
                
                if not valid_selector:
                    logger.warning("未找到有效的文章選擇器，使用默認選擇器")
                    valid_selector = ".search-list-container .search-list-item"
                
                # 獲取目標文章數量
                target_count = float('inf') if article_count is None else start_index + article_count
                
                # 當前已獲取的文章數量
                current_count = 0
                last_count = 0
                consecutive_no_change = 0
                max_no_change_attempts = 3
                
                # 滾動加載直到獲取足夠的文章或無法獲取更多
                while current_count < target_count:
                    # 獲取當前頁面上的文章數量
                    articles = driver.find_elements(By.CSS_SELECTOR, valid_selector)
                    current_count = len(articles)
                    
                    logger.info(f"當前已加載 {current_count} 篇文章")
                    
                    # 如果已經獲取到足夠的文章，跳出循環
                    if current_count >= target_count:
                        logger.info(f"已獲取足夠數量的文章: {current_count} >= {target_count}")
                        break
                    
                    # 檢查是否有新文章加載
                    if current_count == last_count:
                        consecutive_no_change += 1
                        logger.warning(f"滾動後未加載新文章，連續 {consecutive_no_change}/{max_no_change_attempts} 次")
                        
                        # 如果連續多次沒有新文章加載，可能已到達底部
                        if consecutive_no_change >= max_no_change_attempts:
                            logger.info("已連續多次未能加載新文章，可能已到達底部")
                            break
                    else:
                        # 重置計數器
                        consecutive_no_change = 0
                    
                    # 更新上次文章數量
                    last_count = current_count
                    
                    # 滾動到頁面底部
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    logger.info("滾動到頁面底部")
                    
                    # 等待新內容加載
                    time.sleep(2)  # 基礎等待時間
                    
                    # 嘗試檢測加載指示器並等待它消失
                    try:
                        loading_selectors = [".loading", ".load-more", ".infinite-loading", "[class*='loading']"]
                        for loading_selector in loading_selectors:
                            loading_elements = driver.find_elements(By.CSS_SELECTOR, loading_selector)
                            if loading_elements:
                                logger.info(f"檢測到加載指示器: {loading_selector}，等待加載完成")
                                # 等待加載指示器消失
                                WebDriverWait(driver, 10).until_not(
                                    EC.presence_of_element_located((By.CSS_SELECTOR, loading_selector))
                                )
                                break
                    except Exception as e:
                        logger.info(f"等待加載指示器消失時出錯 (可忽略): {e}")
                
                # 獲取渲染後的頁面內容
                rendered_html = driver.page_source
                
                # 保存渲染後的HTML
                html_path = os.path.join(self.output_dir, "search_response_dynamic.html")
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(rendered_html)
                logger.info(f"已保存動態渲染的HTML到 {html_path}")
                
                # 嘗試截圖
                screenshot_path = os.path.join(self.output_dir, "search_screenshot.png")
                driver.save_screenshot(screenshot_path)
                logger.info(f"已保存頁面截圖到 {screenshot_path}")
                
                # 分析渲染後的HTML
                soup = BeautifulSoup(rendered_html, 'html.parser')
                article_elements = soup.select(valid_selector)
                total_articles = len(article_elements)
                logger.info(f"選擇器 '{valid_selector}' 找到 {total_articles} 個結果")
                
                # 應用文章範圍篩選
                end_index = total_articles if article_count is None else min(start_index + article_count, total_articles)
                if start_index >= total_articles:
                    logger.warning(f"起始索引 {start_index} 超出了可用文章數量 {total_articles}")
                    return SearchResult(
                        success=False,
                        method=SearchMethod.DYNAMIC,
                        error_message=f"起始索引 {start_index} 超出了可用文章數量 {total_articles}",
                        total_count=total_articles,
                        response_time=time.time() - start_time
                    )
                
                selected_elements = article_elements[start_index:end_index]
                logger.info(f"根據範圍參數篩選，實際處理文章 {start_index} 至 {end_index-1}，共 {len(selected_elements)} 篇")
                
                # 解析文章信息
                articles = []
                for i, element in enumerate(selected_elements):
                    article = Article(index=start_index + i)
                    
                    # 提取文章標題與URL
                    title_link = element.select_one("a.title") or element.select_one("a") or element.select_one("[href]")
                    if title_link:
                        article.title = title_link.text.strip()
                        article.url = title_link.get('href', '')
                    
                    # 提取文章預覽內容
                    preview = element.select_one("div.desc") or element.select_one("p") or element.select_one(".content")
                    if preview:
                        article.content_preview = preview.text.strip()
                    
                    # 提取作者信息
                    author = element.select_one("a.author") or element.select_one(".blog-nickname") or element.select_one(".user-name")
                    if author:
                        article.author = author.text.strip()
                    
                    # 提取發布日期
                    date = element.select_one("span.date") or element.select_one(".blog-time") or element.select_one(".time")
                    if date:
                        article.publish_date = date.text.strip()
                    
                    # 保存原始HTML
                    article.html = str(element)
                    
                    # 提取所有鏈接
                    links = element.select("a")
                    for link in links:
                        href = link.get('href', '')
                        text = link.text.strip()
                        if href:
                            article.links.append({"href": href, "text": text})
                    
                    articles.append(article)
                
                # 保存文章信息
                articles_info = [
                    {
                        "index": article.index,
                        "title": article.title,
                        "url": article.url,
                        "author": article.author,
                        "publish_date": article.publish_date,
                        "content_preview": article.content_preview[:100] + "..." if len(article.content_preview) > 100 else article.content_preview,
                        "links": article.links
                    }
                    for article in articles
                ]
                
                # 保存文章信息為JSON
                articles_path = os.path.join(self.output_dir, f"articles_{valid_selector.replace('.', '_').replace(' ', '_')}.json")
                with open(articles_path, 'w', encoding='utf-8') as f:
                    json.dump(articles_info, f, ensure_ascii=False, indent=2)
                logger.info(f"已保存文章信息到 {articles_path}")
                
                # 返回成功結果
                return SearchResult(
                    success=True,
                    method=SearchMethod.DYNAMIC,
                    articles=articles,
                    total_count=total_articles,
                    response_time=time.time() - start_time
                )
                
            except WebDriverException as e:
                logger.error(f"WebDriver異常: {e}")
                return SearchResult(
                    success=False,
                    method=SearchMethod.DYNAMIC,
                    error_message=f"WebDriver異常: {str(e)}",
                    response_time=time.time() - start_time
                )
            except Exception as e:
                logger.error(f"動態搜索過程中出錯: {e}", exc_info=True)
                return SearchResult(
                    success=False,
                    method=SearchMethod.DYNAMIC,
                    error_message=f"搜索過程中出錯: {str(e)}",
                    response_time=time.time() - start_time
                )

#-------------------------------------------------------
# 靜態搜索策略實現
#-------------------------------------------------------

class StaticSearchStrategy(SearchStrategy):
    """使用靜態請求進行CSDN搜索的策略實現"""
    
    def __init__(self, output_dir: str = "debug_output"):
        super().__init__(output_dir)
        self.user_agent_manager = UserAgentManager()
        self.rate_limiter = RateLimiter()
    
    def search(self, query: str, start_index: int = 0, article_count: Optional[int] = None) -> SearchResult:
        """靜態搜索策略（實現較簡單，僅作為演示）"""
        logger.warning("靜態搜索方法尚未完全實現，建議使用API或動態搜索方法")
        
        # 應用限速
        self.rate_limiter.wait()
        
        start_time = time.time()
        
        try:
            # 實際上應該實現靜態請求和解析的代碼，這裡僅返回一個空結果
            return SearchResult(
                success=False,
                method=SearchMethod.STATIC,
                error_message="靜態搜索方法尚未實現",
                response_time=time.time() - start_time
            )
        except Exception as e:
            logger.error(f"靜態搜索過程中出錯: {e}", exc_info=True)
            return SearchResult(
                success=False,
                method=SearchMethod.STATIC,
                error_message=f"搜索過程中出錯: {str(e)}",
                response_time=time.time() - start_time
            )

#-------------------------------------------------------
# 綜合搜索控制器
#-------------------------------------------------------

class SearchController:
    """搜索控制器，協調不同搜索策略的執行並匯總結果"""
    
    def __init__(self, output_dir: str = "debug_output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 初始化各種搜索策略
        self.api_strategy = ApiSearchStrategy(output_dir)
        self.dynamic_strategy = DynamicSearchStrategy(output_dir)
        self.static_strategy = StaticSearchStrategy(output_dir)
    
    def search(self, query: str, method: SearchMethod = SearchMethod.ALL, 
               start_index: int = 0, article_count: Optional[int] = None) -> Dict[str, Any]:
        """
        執行搜索操作，選擇性地使用多種搜索策略
        
        參數:
            query: 搜索關鍵詞
            method: 搜索方法（ALL, API, DYNAMIC, STATIC）
            start_index: 開始的文章索引（從0開始）
            article_count: 要獲取的文章數量
            
        返回:
            dict: 包含搜索結果和建議的字典
        """
        logger.info("=" * 50)
        logger.info(f"開始CSDN搜索: {query} [方法: {method.name}, 文章範圍: {start_index} 起始, 數量 {article_count if article_count else '全部'}]")
        logger.info("=" * 50)
        
        results = {}
        
        # 根據選擇的方法執行搜索
        if method in [SearchMethod.ALL, SearchMethod.API]:
            logger.info("執行API搜索")
            results["api"] = self.api_strategy.search(query, start_index, article_count)
        
        if method in [SearchMethod.ALL, SearchMethod.DYNAMIC]:
            logger.info("執行動態搜索")
            results["dynamic"] = self.dynamic_strategy.search(query, start_index, article_count)
        
        if method in [SearchMethod.ALL, SearchMethod.STATIC]:
            logger.info("執行靜態搜索")
            results["static"] = self.static_strategy.search(query, start_index, article_count)
        
        # 匯總結果
        summary = self._create_summary(query, results, start_index, article_count)
        
        # 保存搜索結果匯總
        results_path = os.path.join(self.output_dir, "search_summary.json")
        with open(results_path, 'w', encoding='utf-8') as f:
            # 將SearchResult對象轉換為可序列化的字典
            serializable_summary = self._make_serializable(summary)
            json.dump(serializable_summary, f, ensure_ascii=False, indent=2)
        logger.info(f"已保存搜索匯總到 {results_path}")
        
        logger.info("=" * 50)
        logger.info(f"搜索完成: {summary['recommendation']}")
        logger.info("=" * 50)
        
        return summary
    
    def _create_summary(self, query: str, results: Dict[str, SearchResult], 
                       start_index: int, article_count: Optional[int]) -> Dict[str, Any]:
        """創建搜索結果匯總"""
        # 準備初始匯總結構
        summary = {
            "query": query,
            "article_range": {
                "start_index": start_index,
                "article_count": article_count
            },
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "methods": {},
            "recommendation": None,
            "best_result": None
        }
        
        # 填充各方法的結果
        for method_name, result in results.items():
            summary["methods"][method_name] = {
                "success": result.success,
                "response_time": result.response_time,
                "total_count": result.total_count,
                "article_count": len(result.articles) if result.articles else 0,
                "error_message": result.error_message
            }
        
        # 決定最佳結果
        best_method = None
        if "dynamic" in results and results["dynamic"].success:
            best_method = "dynamic"
        elif "api" in results and results["api"].success:
            best_method = "api"
        elif "static" in results and results["static"].success:
            best_method = "static"
        
        # 設定推薦方法
        if best_method:
            summary["best_result"] = best_method
            if best_method == "dynamic":
                summary["recommendation"] = "動態渲染方法成功，建議使用Selenium進行後續爬取"
            elif best_method == "api":
                summary["recommendation"] = "API請求方法成功，建議直接調用API進行後續爬取"
            elif best_method == "static":
                summary["recommendation"] = "僅靜態請求成功，但可能無法獲取完整內容，建議改用動態渲染方法"
        else:
            summary["recommendation"] = "所有方法均失敗，建議檢查網絡連接或嘗試其他策略"
        
        return summary
    
    def _make_serializable(self, obj):
        """將對象轉換為可序列化的格式"""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, (SearchResult, Article)):
            return {k: self._make_serializable(v) for k, v in obj.__dict__.items()}
        elif isinstance(obj, SearchMethod):
            return obj.name
        else:
            return obj

#-------------------------------------------------------
# 命令行接口
#-------------------------------------------------------

def parse_arguments():
    """解析命令行參數"""
    parser = argparse.ArgumentParser(description="CSDN 搜索爬蟲")
    parser.add_argument("--query", required=True, help="搜索查詢詞")
    parser.add_argument("--output-dir", default="debug_output", help="輸出目錄")
    parser.add_argument("--method", choices=["all", "static", "dynamic", "api"], default="all", help="搜索方法")
    parser.add_argument("--start", type=int, default=0, help="開始爬取的文章索引（從0開始）")
    parser.add_argument("--count", type=int, default=None, help="要爬取的文章數量，不指定則爬取所有可獲取的文章")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="日誌記錄級別")
    
    return parser.parse_args()

def main():
    """主函數"""
    # 解析命令行參數
    args = parse_arguments()
    
    # 設置日誌級別
    logger.setLevel(getattr(logging, args.log_level))
    
    # 獲取搜索方法枚舉值
    method_map = {
        "all": SearchMethod.ALL,
        "static": SearchMethod.STATIC,
        "dynamic": SearchMethod.DYNAMIC,
        "api": SearchMethod.API
    }
    search_method = method_map[args.method]
    
    # 創建搜索控制器並執行搜索
    controller = SearchController(output_dir=args.output_dir)
    results = controller.search(
        query=args.query,
        method=search_method,
        start_index=args.start,
        article_count=args.count
    )
    
    # 顯示搜索結果摘要
    print("\n================ 搜索結果摘要 ================")
    print(f"查詢詞: {args.query}")
    print(f"搜索方法: {args.method}")
    print(f"文章範圍: 從 {args.start} 開始，獲取 {args.count if args.count else '全部'} 篇文章")
    print(f"推薦: {results['recommendation']}")
    
    for method, result in results["methods"].items():
        status = "成功" if result["success"] else "失敗"
        print(f"\n{method.upper()} 方法: {status}")
        if result["success"]:
            print(f"  - 找到文章: {result['article_count']} 篇")
            print(f"  - 總文章數: {result['total_count']} 篇")
            print(f"  - 響應時間: {result['response_time']:.2f} 秒")
        else:
            print(f"  - 錯誤: {result['error_message']}")
    
    print("\n結果已保存到: ", os.path.abspath(args.output_dir))
    print("=================================================\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("程序被用戶中斷")
        print("\n程序已中斷")
    except Exception as e:
        logger.error(f"程序執行過程中發生未處理的異常: {e}", exc_info=True)
        print(f"\n程序執行出錯: {e}")
        print("詳細錯誤信息已記錄到日誌文件")
    finally:
        logging.shutdown()