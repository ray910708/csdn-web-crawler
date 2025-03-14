#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
改进版的 CSDN 搜索爬虫 - 处理SPA架构
支持选择爬取文章的起始位置和数量，并实现实时保存 JSON 数据和文章查重
新增：异步多线程支持、文章监控、实时保存和查重功能
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import logging
import json
import os
import argparse
import math
import hashlib
from urllib.parse import quote
from typing import Dict, Set, List
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from dataclasses import dataclass, asdict
from datetime import datetime
import queue
from pathlib import Path
import gzip
import psutil  # 若未安裝，需執行 pip install psutil
import glob
import re
import shutil

# 尝试导入selenium相关库
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning("Selenium未安装，将无法处理动态内容。建议执行: pip install selenium webdriver-manager")

# 配置日志记录
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("csdn_debug.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("csdn_debug_scraper")

# 修复Windows控制台输出编码问题
import sys
if sys.platform == 'win32':
    import codecs
    # 尝试修复控制台输出编码
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python 3.7 或更早版本
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer)
    
    # 为所有处理程序设置编码
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setStream(sys.stdout)

# 全局计数器，用于自适应请求策略
request_count = 0

def generate_headers() -> Dict[str, str]:
    """生成随机化的请求头，降低被拦截风险"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
    ]
    
    return {
        "User-Agent": random.choice(user_agents),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8", 
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": random.choice(["keep-alive", "close"]),
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": random.choice(["max-age=0", "no-cache"]),
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1"
    }

def adaptive_delay() -> None:
    """实现自适应延迟策略，避免触发反爬机制"""
    global request_count
    base_delay = 2
    jitter = random.uniform(0.5, 1.5)
    delay = base_delay * jitter
    
    # 指数退避算法
    if request_count > 10:
        delay *= math.log(request_count, 2)
    
    time.sleep(delay)
    request_count += 1
    logger.debug(f"等待 {delay:.2f} 秒")

def debug_csdn_search_dynamic(query: str, output_dir: str = "debug_output", start_index: int = 0, article_count: int = None) -> bool:
    """
    使用Selenium动态渲染CSDN搜索结果，支持无限滚动加载更多文章，
    并在文章获取后实时保存为 JSON Lines 格式，同时对文章进行查重
    新增：异步多线程支持和文章监控功能
    """
    if not SELENIUM_AVAILABLE:
        logger.error("无法使用动态请求方式，因为Selenium未安装")
        return False
    
    logger.info(f"开始使用动态请求方式调试 CSDN 搜索: {query} [文章范围: {start_index} 起始, 数量 {article_count if article_count else '全部'}]")
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 初始化文章监控器
    monitor = ArticleMonitor(output_dir)
    # 初始化异步文章获取器
    fetcher = AsyncArticleFetcher(monitor, max_workers=args.workers if 'args' in globals() else 5)
    
    # 增加最大尝试页数
    max_pages = args.max_pages if 'args' in globals() and hasattr(args, 'max_pages') else 20  # 最多尝试20页
    article_urls = []
    processed_urls = set()  # 用于跟踪已处理的URL
    url_discovery_times = {}  # 用於跟蹤URL發現時間
    
    # 创建异步事件循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # 创建一个任务队列，用于存放待处理的URL
    url_queue = asyncio.Queue()
    
    # 创建URL备份文件路径
    url_backup_file = os.path.join(output_dir, "collected_urls.txt")
    url_backup_file_json = os.path.join(output_dir, "collected_urls.json")
    remaining_urls_file = os.path.join(output_dir, "remaining_urls.json")
    
    # 检查是否有之前未完成的URL列表
    resume_mode = False
    if os.path.exists(remaining_urls_file):
        try:
            with open(remaining_urls_file, "r", encoding="utf-8") as f:
                remaining_data = json.load(f)
                remaining_urls = remaining_data.get("urls", [])
                if remaining_urls and len(remaining_urls) > 0:
                    resume_mode = True
                    logger.info(f"發現未完成的任務，包含 {len(remaining_urls)} 個URL")
                    
                    # 詢問用戶是否要繼續處理
                    if 'args' in globals() and hasattr(args, 'resume') and args.resume:
                        logger.info("自動繼續處理未完成的URL")
                    else:
                        # 如果沒有指定自動恢復，仍然可以處理，但會發出警告
                        logger.warning("將自動繼續處理未完成的URL，使用 --no-resume 可禁用此行為")
                    
                    # 將未處理的URL添加到收集列表和發現時間字典
                    original_query = remaining_data.get("query", "")
                    if original_query and original_query != query:
                        logger.warning(f"注意：恢復的URL來自不同的查詢詞 '{original_query}'，當前查詢詞為 '{query}'")
                    
                    # 加載未處理的URL
                    for url in remaining_urls:
                        if url not in article_urls:
                            article_urls.append(url)
                            # 設置一個默認的發現時間
                            url_discovery_times[url] = remaining_data.get("timestamp", datetime.now().isoformat())
                    
                    logger.info(f"已從未完成任務加載 {len(article_urls)} 個URL")
                    
                    # 如果參數中提供了article_count，檢查是否需要調整
                    if article_count is not None and len(article_urls) > article_count:
                        logger.warning(f"URL數量 ({len(article_urls)}) 超過指定的數量 ({article_count})，將只處理前 {article_count} 個")
                        article_urls = article_urls[:article_count]
        except Exception as e:
            logger.error(f"加載未完成任務時出錯: {e}")
            resume_mode = False
    
    # 从备份文件加载之前收集的URL
    if not resume_mode:
        try:
            recover_urls_from_backups(url_backup_file, url_backup_file_json, article_urls, url_discovery_times, query)
        except Exception as e:
            logger.error(f"加载备份URL时出错: {e}")
    
    # 启动异步处理任务
    async def process_url_queue():
        await fetcher.create_session()
        processing_count = 0
        while True:
            try:
                url = await asyncio.wait_for(url_queue.get(), timeout=30)  # 添加超时
                if url is None:  # 结束信号
                    break
                
                # 获取当前索引
                index = len(processed_urls)
                try:
                    await fetcher.fetch_article(url, index)
                    processed_urls.add(url)
                    processing_count += 1
                    
                    # 每处理10个URL，更新一次统计信息
                    if processing_count % 10 == 0:
                        monitor.update_stats()
                        logger.info(f"实时统计: 已处理 {len(processed_urls)} 个URL, 成功: {fetcher.success_count}, 重复: {fetcher.duplicate_count}, 失败: {fetcher.failed_count}")
                except Exception as e:
                    logger.error(f"处理URL时出错: {url}, 错误: {e}")
                    # 将失败的URL重新加入队列，最多重试3次
                    if not hasattr(url, '_retry_count'):
                        url._retry_count = 0
                    if url._retry_count < 3:
                        url._retry_count += 1
                        logger.info(f"URL {url} 将重试，第 {url._retry_count} 次")
                        await url_queue.put(url)
                finally:
                    url_queue.task_done()
            except asyncio.TimeoutError:
                logger.warning("等待URL队列超时，检查是否有新URL")
                continue
            except Exception as e:
                logger.error(f"URL队列处理器出错: {e}")
                await asyncio.sleep(1)  # 出错后短暂暂停
    
    # 启动异步处理任务
    processing_task = loop.create_task(process_url_queue())
    
    # 保存URL到备份文件的函数
    def save_urls_to_backup():
        try:
            with open(url_backup_file, "w", encoding="utf-8") as f:
                for url in article_urls:
                    f.write(f"{url}\n")
            logger.info(f"已将 {len(article_urls)} 个URL保存到备份文件")
        except Exception as e:
            logger.error(f"保存URL到备份文件时出错: {e}")
    
    # 保存URL到JSON备份文件的函数
    def save_urls_to_backup_json():
        try:
            # 构建备份数据结构
            backup_data = {
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "total_urls": len(article_urls),
                    "query": query,
                    "version": "1.0",
                    "scraping_stats": {
                        "processed": len(processed_urls),
                        "success_count": fetcher.success_count,
                        "duplicate_count": fetcher.duplicate_count,
                        "failed_count": fetcher.failed_count
                    }
                },
                "urls": [
                    {
                        "url": url,
                        "discovered_at": url_discovery_times.get(url, datetime.now().isoformat()),
                        "processed": url in processed_urls,
                        "retry_count": getattr(url, '_retry_count', 0) if hasattr(url, '_retry_count') else 0
                    } for url in article_urls
                ]
            }
            
            # 使用JSON格式写入文件
            with open(url_backup_file_json, "w", encoding="utf-8") as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"已将 {len(article_urls)} 个URL以JSON格式保存到备份文件")
        except Exception as e:
            logger.error(f"保存URL到JSON備份文件時出錯: {e}")
            # 尝试使用文本格式作为后备方案
            save_urls_to_backup()
    
    # 定期保存URL的函数
    def periodic_url_backup():
        while True:
            time.sleep(30)  # 每30秒保存一次
            # 根据URL数量选择合适的备份策略
            if len(article_urls) > 10000:
                compressed_json_backup()
            elif len(article_urls) > 1000:
                incremental_json_backup()
            else:
                save_urls_to_backup_json()
                # 同时保留文本备份作为后备
                save_urls_to_backup()
    
    # 启动定期保存URL的线程
    import threading
    backup_thread = threading.Thread(target=periodic_url_backup, daemon=True)
    backup_thread.start()
    
    # 增量JSON备份
    def incremental_json_backup():
        # 仅序列化新添加的URL
        if not hasattr(incremental_json_backup, 'last_backup_count'):
            incremental_json_backup.last_backup_count = 0
        
        # 获取新增URL
        new_urls = article_urls[incremental_json_backup.last_backup_count:]
        if not new_urls:
            return  # 无新URL，跳过备份
            
        try:
            # 读取现有备份
            existing_data = {}
            if os.path.exists(url_backup_file_json):
                with open(url_backup_file_json, "r", encoding="utf-8") as f:
                    existing_data = json.load(f)
            
            # 更新元数据和URL列表
            if not existing_data:
                existing_data = {
                    "metadata": {
                        "timestamp": datetime.now().isoformat(),
                        "total_urls": len(article_urls),
                        "query": query,
                        "version": "1.0",
                        "scraping_stats": {
                            "processed": len(processed_urls),
                            "success_count": fetcher.success_count,
                            "duplicate_count": fetcher.duplicate_count,
                            "failed_count": fetcher.failed_count
                        }
                    },
                    "urls": []
                }
            else:
                if "metadata" in existing_data:
                    existing_data["metadata"].update({
                        "timestamp": datetime.now().isoformat(),
                        "total_urls": len(article_urls),
                        "scraping_stats": {
                            "processed": len(processed_urls),
                            "success_count": fetcher.success_count,
                            "duplicate_count": fetcher.duplicate_count,
                            "failed_count": fetcher.failed_count
                        }
                    })
                else:
                    existing_data["metadata"] = {
                        "timestamp": datetime.now().isoformat(),
                        "total_urls": len(article_urls),
                        "query": query,
                        "version": "1.0",
                        "scraping_stats": {
                            "processed": len(processed_urls),
                            "success_count": fetcher.success_count,
                            "duplicate_count": fetcher.duplicate_count,
                            "failed_count": fetcher.failed_count
                        }
                    }
            
            # 添加新URL
            new_url_objects = [
                {
                    "url": url,
                    "discovered_at": url_discovery_times.get(url, datetime.now().isoformat()),
                    "processed": url in processed_urls,
                    "retry_count": getattr(url, '_retry_count', 0) if hasattr(url, '_retry_count') else 0
                } for url in new_urls
            ]
            
            if "urls" not in existing_data:
                existing_data["urls"] = []
                
            existing_data["urls"].extend(new_url_objects)
            
            # 写入更新后的文件
            with open(url_backup_file_json, "w", encoding="utf-8") as f:
                json.dump(existing_data, f, ensure_ascii=False, indent=2)
                
            incremental_json_backup.last_backup_count = len(article_urls)
            logger.info(f"增量备份：新增 {len(new_urls)} 个URL，总计 {len(article_urls)} 个")
        except Exception as e:
            logger.error(f"增量JSON备份时出错: {e}")
            # 尝试使用完整备份作为后备方案
            save_urls_to_backup_json()
    
    # 压缩JSON备份
    def compressed_json_backup():
        try:
            import gzip
            
            backup_data = {
                "metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "total_urls": len(article_urls),
                    "query": query,
                    "compressed": True,
                    "version": "1.1",
                    "scraping_stats": {
                        "processed": len(processed_urls),
                        "success_count": fetcher.success_count,
                        "duplicate_count": fetcher.duplicate_count,
                        "failed_count": fetcher.failed_count
                    }
                },
                "urls": [
                    {
                        "url": url,
                        "discovered_at": url_discovery_times.get(url, datetime.now().isoformat()),
                        "processed": url in processed_urls,
                        "retry_count": getattr(url, '_retry_count', 0) if hasattr(url, '_retry_count') else 0
                    } for url in article_urls
                ]
            }
            
            # 序列化为JSON字符串
            json_str = json.dumps(backup_data, ensure_ascii=False)
            
            # 使用gzip压缩
            compressed_file = url_backup_file_json + ".gz"
            with gzip.open(compressed_file, "wt", encoding="utf-8") as f:
                f.write(json_str)
                
            logger.info(f"已将 {len(article_urls)} 个URL以压缩JSON格式保存")
        except Exception as e:
            logger.error(f"压缩JSON备份时出错: {e}")
            # 尝试非压缩备份
            save_urls_to_backup_json()
    
    try:
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument(f'user-agent={generate_headers()["User-Agent"]}')
        
        driver = webdriver.Chrome(options=options)
        try:
            # 增加分页循环
            for page in range(1, max_pages + 1):
                if article_count is not None and len(article_urls) >= article_count:
                    logger.info(f"已收集足够数量的URL: {len(article_urls)} >= {article_count}")
                    break
                    
                encoded_query = quote(query)
                # 添加页码参数
                search_url = f"https://so.csdn.net/so/search/s.do?q={encoded_query}&t=blog&p={page}"
                logger.info(f"使用Selenium请求第 {page} 页: {search_url}")
                driver.get(search_url)
                time.sleep(5)  # 初始等待
                
                if "CSDN" not in driver.title:
                    logger.warning(f"页面可能未正确加载，标题为: {driver.title}")
                    time.sleep(5)
                
                selectors = [
                    ".search-list-box .list-item",
                    ".search-list-box .search-list-item",
                    ".so-search-box .list-item",
                    "div[class*='search-list'] div[class*='item']",
                    "div[class*='result-item']",
                    "div[class*='article-item']",
                    "div[class*='blog-item']",
                    "div[class*='list-item']",
                    "div[class*='search-item']"
                ]
                
                valid_selector = None
                for selector in selectors:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements and any(element.is_displayed() for element in elements):
                            valid_selector = selector
                            logger.info(f"找到有效选择器：{valid_selector}")
                            break
                    except Exception as e:
                        continue
                
                if not valid_selector:
                    logger.warning("未找到有效的文章选择器，使用默认选择器")
                    valid_selector = ".search-list-box .list-item"
                
                # 增加滚动尝试次数
                max_no_change_attempts = 10  # 从3增加到10
                current_count = 0
                last_count = 0
                consecutive_no_change = 0
                load_more_clicks = 0
                max_load_more_clicks = 30
                
                # 在当前页面上滚动加载更多文章
                page_urls_before = len(article_urls)
                
                # 滚动加载当前页面的所有文章
                for scroll_attempt in range(20):  # 最多滚动20次
                    articles_elements = driver.find_elements(By.CSS_SELECTOR, valid_selector)
                    current_count = len(articles_elements)
                    logger.info(f"当前页面已找到 {current_count} 篇文章")
                    
                    # 收集新文章的URL並立即加入處理隊列
                    new_urls_found = 0
                    for element in articles_elements:
                        try:
                            link = element.find_element(By.CSS_SELECTOR, "a[href*='blog.csdn.net']")
                            article_url = link.get_attribute('href')
                            if article_url and article_url not in article_urls:
                                article_urls.append(article_url)
                                # 記錄 URL 發現時間
                                url_discovery_times[article_url] = datetime.now().isoformat()
                                # 立即將URL加入處理隊列
                                if article_url not in processed_urls:
                                    loop.run_until_complete(url_queue.put(article_url))
                                    new_urls_found += 1
                                    logger.debug(f"新增URL並加入隊列: {article_url}")
                        except Exception as e:
                            logger.debug(f"URL提取錯誤 (可忽略): {str(e)[:100]}")
                            continue
                    
                    logger.info(f"當前已收集 {len(article_urls)} 個有效文章URL，本次新增 {new_urls_found} 個")
                    
                    # 每次找到新URL就保存一次備份並輸出詳細統計
                    if new_urls_found > 0:
                        if len(article_urls) > 10000:
                            compressed_json_backup()
                        elif len(article_urls) > 1000:
                            incremental_json_backup()
                        else:
                            save_urls_to_backup_json()
                            
                        # 輸出URL收集的詳細統計
                        processed_count = len(processed_urls)
                        logger.info("-" * 40)
                        logger.info(f"URL收集統計: 總URL {len(article_urls)}, 已處理 {processed_count}, "
                                  f"剩餘 {len(article_urls) - processed_count}")
                        logger.info(f"處理結果: 成功 {fetcher.success_count}, 重複 {fetcher.duplicate_count}, "
                                  f"失敗 {fetcher.failed_count}")
                        logger.info("-" * 40)
                    
                    if article_count is not None and len(article_urls) >= article_count:
                        logger.info(f"已收集足够数量的URL: {len(article_urls)} >= {article_count}")
                        break
                    
                    if current_count == last_count:
                        consecutive_no_change += 1
                        logger.warning(f"滚动后未加载新文章，连续 {consecutive_no_change}/{max_no_change_attempts} 次")
                        
                        # 尝试点击"加载更多"按钮
                        load_more_buttons = [
                            "div[data-v-134691f0][data-v-6626c741][class='so-load-data']",
                            ".so-load-data",
                            "div[class*='load-more']",
                            "div[class*='load-data']",
                            "button[class*='load-more']",
                            "div[class*='load']",
                            "button[class*='load']",
                            "div[class*='more']",
                            "button[class*='more']"
                        ]
                        button_clicked = False
                        for button_selector in load_more_buttons:
                            try:
                                logger.info(f"尝试查找加载更多按钮: {button_selector}")
                                load_more_elements = driver.find_elements(By.CSS_SELECTOR, button_selector)
                                visible_buttons = [btn for btn in load_more_elements if btn.is_displayed()]
                                if visible_buttons:
                                    button = visible_buttons[0]
                                    logger.info(f"找到可见的'加载更多'按钮: {button_selector}")
                                    driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", button)
                                    time.sleep(2)
                                    click_success = False
                                    try:
                                        driver.execute_script("arguments[0].click();", button)
                                        logger.info("使用JavaScript点击'加载更多'按钮成功")
                                        click_success = True
                                    except Exception as e1:
                                        logger.info(f"JavaScript点击失败: {e1}")
                                    if not click_success:
                                        try:
                                            actions = webdriver.ActionChains(driver)
                                            actions.move_to_element(button).click().perform()
                                            logger.info("使用Actions链点击'加载更多'按钮成功")
                                            click_success = True
                                        except Exception as e2:
                                            logger.info(f"Actions链点击失败: {e2}")
                                    if not click_success:
                                        try:
                                            button.click()
                                            logger.info("使用直接点击'加载更多'按钮成功")
                                            click_success = True
                                        except Exception as e3:
                                            logger.info(f"直接点击失败: {e3}")
                                    if click_success:
                                        load_more_clicks += 1
                                        logger.info(f"点击'加载更多'按钮成功，第 {load_more_clicks} 次")
                                        button_clicked = True
                                        consecutive_no_change = 0
                                        time.sleep(3)
                                        break
                                else:
                                    logger.info(f"未找到可见的'加载更多'按钮: {button_selector}")
                            except Exception as e:
                                logger.info(f"尝试点击按钮 {button_selector} 时出错: {e}")
                        
                        if not button_clicked:
                            if consecutive_no_change >= max_no_change_attempts or load_more_clicks >= max_load_more_clicks:
                                logger.info("已连续多次未能加载新文章，且找不到'加载更多'按钮，可能已到达底部")
                                break
                    else:
                        consecutive_no_change = 0
                    
                    last_count = current_count
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    logger.info("滚动到页面底部")
                    time.sleep(3)
                    
                    # 检查是否有加载更多按钮
                    try:
                        load_more_visible = False
                        for button_selector in load_more_buttons:
                            load_more_elements = driver.find_elements(By.CSS_SELECTOR, button_selector)
                            if load_more_elements and load_more_elements[0].is_displayed():
                                load_more_visible = True
                                logger.info(f"滚动后发现'加载更多'按钮: {button_selector}")
                                break
                        if not load_more_visible:
                            logger.info("未发现'加载更多'按钮，等待自动加载")
                            time.sleep(2)
                    except Exception as e:
                        logger.info(f"检查'加载更多'按钮时出错 (可忽略): {e}")
                    
                    # 等待加载指示器消失
                    try:
                        loading_selectors = [
                            "div.loading",
                            "div.loading-box",
                            "div[class*='loading']",
                            "div.infinite-loading"
                        ]
                        loading_wait_start = time.time()
                        loading_timeout = 15
                        for loading_selector in loading_selectors:
                            loading_elements = driver.find_elements(By.CSS_SELECTOR, loading_selector)
                            if loading_elements and loading_elements[0].is_displayed():
                                logger.info(f"检测到加载指示器: {loading_selector}，等待加载完成")
                                try:
                                    WebDriverWait(driver, 5).until_not(
                                        EC.visibility_of_element_located((By.CSS_SELECTOR, loading_selector))
                                    )
                                    logger.info("加载指示器已消失")
                                    break
                                except TimeoutException:
                                    if time.time() - loading_wait_start > loading_timeout:
                                        logger.warning("加载指示器等待超时，继续处理")
                                        break
                                    continue
                    except Exception as e:
                        logger.info(f"等待加载指示器消失时出错 (可忽略): {e}")
                
                page_urls_after = len(article_urls)
                urls_from_this_page = page_urls_after - page_urls_before
                logger.info(f"从第 {page} 页收集到 {urls_from_this_page} 个URL")
                
                # 如果当前页面没有新URL，可能已经到达最后一页
                if urls_from_this_page == 0:
                    logger.info(f"第 {page} 页没有新URL，可能已到达最后一页")
                    # 尝试检查是否有下一页按钮
                    try:
                        next_page_selectors = [
                            "a.ui-pager-next",
                            "a.next",
                            "a[class*='next']",
                            "li.next a",
                            "div.page-box a:last-child"
                        ]
                        has_next = False
                        for next_selector in next_page_selectors:
                            next_buttons = driver.find_elements(By.CSS_SELECTOR, next_selector)
                            if next_buttons and next_buttons[0].is_displayed():
                                has_next = True
                                break
                        if not has_next:
                            logger.info("未找到下一页按钮，停止翻页")
                            break
                    except Exception as e:
                        logger.warning(f"检查下一页按钮时出错: {e}")
            
        except Exception as e:
            logger.error(f"Selenium会话出错: {e}", exc_info=True)
        finally:
            # 保存最后收集的URL
            save_urls_to_backup()
            try:
                driver.quit()
            except Exception as e:
                logger.error(f"关闭Selenium驱动时出错: {e}")
    
    except Exception as e:
        logger.error(f"动态请求调试过程中出错: {e}", exc_info=True)
    
    # 确保所有收集的URL都被处理，即使Selenium会话中断
    logger.info(f"Selenium会话结束，确保处理所有收集的URL: {len(article_urls)} 个")
    
    # 将所有收集但未处理的URL加入队列
    unprocessed_urls = [url for url in article_urls if url not in processed_urls]
    logger.info(f"发现 {len(unprocessed_urls)} 个未处理的URL，现在开始处理")
    
    try:
        # 将未处理的URL加入队列
        for url in unprocessed_urls:
            loop.run_until_complete(url_queue.put(url))
        
        # 等待所有URL处理完成
        logger.info("等待队列中的URL处理完成...")
        loop.run_until_complete(url_queue.put(None))  # 发送结束信号
        
        # 設置更長的超時時間，並添加進度報告
        timeout_min = args.url_timeout if hasattr(args, 'url_timeout') and args.url_timeout > 0 else 30  # 預設30分鐘
        timeout_sec = timeout_min * 60
        logger.info(f"設置URL處理超時為 {timeout_min} 分鐘")
        
        # 使用分段超時處理，每段時間檢查一次進度
        total_waited = 0
        progress_check_interval = 60  # 每60秒檢查一次進度
        last_processed_count = len(processed_urls)
        stalled_count = 0
        max_stalled_times = 5  # 最多允許連續5次無進度
        
        try:
            while not processing_task.done():
                try:
                    # 等待一小段時間，不是整個超時時間
                    loop.run_until_complete(asyncio.wait_for(asyncio.shield(processing_task), timeout=progress_check_interval))
                    break  # 如果任務完成，跳出循環
                except asyncio.TimeoutError:
                    # 檢查這段時間内的處理進度
                    total_waited += progress_check_interval
                    current_processed_count = len(processed_urls)
                    newly_processed = current_processed_count - last_processed_count
                    
                    if newly_processed > 0:
                        # 有進度，重置停滯計數
                        stalled_count = 0
                        logger.info(f"URL處理進行中: {current_processed_count}/{len(article_urls)} URLs "
                                   f"({newly_processed} 個新處理, {total_waited/60:.1f}/{timeout_min} 分鐘)")
                    else:
                        # 無進度，增加停滯計數
                        stalled_count += 1
                        logger.warning(f"URL處理停滯 {stalled_count}/{max_stalled_times}: "
                                      f"仍在 {current_processed_count}/{len(article_urls)} "
                                      f"({total_waited/60:.1f}/{timeout_min} 分鐘)")
                        
                        # 如果連續多次無進度，可能是卡住了
                        if stalled_count >= max_stalled_times:
                            logger.warning(f"URL處理連續 {max_stalled_times} 次無進度，嘗試保存並退出")
                            break
                    
                    # 保存當前進度
                    if newly_processed > 0:
                        try:
                            save_urls_to_backup_json()
                        except Exception as e:
                            logger.error(f"保存進度時出錯: {e}")
                    
                    # 更新基準值
                    last_processed_count = current_processed_count
                    
                    # 檢查總超時
                    if total_waited >= timeout_sec:
                        logger.warning(f"URL處理總時間達到 {timeout_min} 分鐘，強制退出")
                        break
            
            # 如果任務未完成，取消它
            if not processing_task.done():
                processing_task.cancel()
                try:
                    loop.run_until_complete(processing_task)
                except asyncio.CancelledError:
                    logger.info("URL處理任務已取消")
                    
            # 計算和記錄處理統計
            processed_ratio = len(processed_urls) / len(article_urls) * 100
            logger.info(f"URL處理結束: 完成度 {processed_ratio:.1f}% ({len(processed_urls)}/{len(article_urls)}), "
                       f"耗時 {total_waited/60:.1f} 分鐘")
            
        except Exception as e:
            logger.error(f"監控URL處理進度時出錯: {e}", exc_info=True)
        
        # 更新最终统计信息
        monitor.update_stats()
        
        logger.info(f"文章處理完成狀態: 總計處理 {monitor.total_articles} 篇文章，"
                  f"保存 {monitor.saved_articles} 篇唯一文章，"
                  f"發現 {monitor.duplicate_articles} 篇重複文章，"
                  f"完成度 {len(processed_urls)}/{len(article_urls)} ({processed_ratio:.1f}%)")
        
        # 保存未完成URL列表，便於下次繼續
        if len(processed_urls) < len(article_urls):
            remaining_urls = [url for url in article_urls if url not in processed_urls]
            remaining_file = os.path.join(output_dir, "remaining_urls.json")
            try:
                with open(remaining_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "timestamp": datetime.now().isoformat(),
                        "query": query,
                        "total_remaining": len(remaining_urls),
                        "urls": remaining_urls
                    }, f, ensure_ascii=False, indent=2)
                logger.info(f"已保存 {len(remaining_urls)} 個未完成的URL到 {remaining_file}")
            except Exception as e:
                logger.error(f"保存未完成URL列表時出錯: {e}")
                
        return len(processed_urls) > 0  # 只要處理了一些URL就返回成功
    except Exception as e:
        logger.error(f"处理剩余URL时出错: {e}", exc_info=True)
        return False
    finally:
        # 關閉異步事件循環
        try:
            loop.run_until_complete(fetcher.close_session())
            loop.close()
        except Exception as e:
            logger.error(f"关闭异步事件循环时出错: {e}")
            
def debug_csdn_search_api(query: str, output_dir: str = "debug_output", start_index: int = 0, article_count: int = None) -> bool:
    """尝试直接调用CSDN搜索API，可指定文章范围"""
    logger.info(f"开始尝试API请求调试 CSDN 搜索: {query} [文章范围: {start_index} 起始, 数量 {article_count if article_count else '全部'}]")
    
    os.makedirs(output_dir, exist_ok=True)
    headers = generate_headers()
    headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://so.csdn.net/so/search"
    })
    
    try:
        encoded_query = quote(query)
        page = start_index // 20 + 1
        api_endpoints = [
            f"https://so.csdn.net/api/v3/search?q={encoded_query}&t=all&p={page}&s=0&tm=0",
            f"https://so.csdn.net/so/search/api/searchV2?q={encoded_query}&t=all&p={page}&s=0&tm=0",
            f"https://so.csdn.net/so/search?q={encoded_query}&t=all&p={page}&s=0&tm=0&format=json"
        ]
        
        for endpoint in api_endpoints:
            logger.info(f"尝试API端点: {endpoint}")
            adaptive_delay()
            response = requests.get(endpoint, headers=headers, timeout=30)
            try:
                json_data = response.json()
                api_path = os.path.join(output_dir, f"api_response_{api_endpoints.index(endpoint)}.json")
                with open(api_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                logger.info(f"已保存API响应到 {api_path}")
                
                if isinstance(json_data, dict):
                    for data_field in ['result_vos', 'data', 'results', 'items']:
                        if data_field in json_data:
                            all_items = json_data[data_field]
                            in_page_offset = start_index % 20
                            end_offset = len(all_items) if article_count is None else min(in_page_offset + article_count, len(all_items))
                            filtered_items = all_items[in_page_offset:end_offset]
                            filtered_response = json_data.copy()
                            filtered_response[data_field] = filtered_items
                            filtered_api_path = os.path.join(output_dir, f"filtered_api_response_{api_endpoints.index(endpoint)}.json")
                            with open(filtered_api_path, 'w', encoding='utf-8') as f:
                                json.dump(filtered_response, f, ensure_ascii=False, indent=2)
                            logger.info(f"已保存筛选后的API响应到 {filtered_api_path}，包含 {len(filtered_items)} 篇文章")
                            return True
                    logger.warning("API响应中未找到预期的数据字段")
            except json.JSONDecodeError:
                logger.info(f"端点不返回JSON: {endpoint}")
                continue
        
        logger.warning("所有API端点尝试失败")
        return False
        
    except Exception as e:
        logger.error(f"API调试过程中出错: {e}", exc_info=True)
        return False

def debug_csdn_search(query: str, output_dir: str = "debug_output", start_index: int = 0, article_count: int = None) -> None:
    """综合调试CSDN搜索，尝试多种方法，可指定文章范围"""
    logger.info("=" * 50)
    logger.info(f"开始综合调试 CSDN 搜索: {query} [文章范围: {start_index} 起始, 数量 {article_count if article_count else '全部'}]")
    logger.info("=" * 50)
    
    logger.info("步骤1: 尝试静态请求方法")
    static_result = False  # 静态请求方法未实现
    logger.info("步骤2: 尝试API请求方法")
    api_result = debug_csdn_search_api(query, output_dir, start_index, article_count)
    logger.info("步骤3: 尝试动态渲染方法")
    dynamic_result = debug_csdn_search_dynamic(query, output_dir, start_index, article_count)
    
    results = {
        "query": query,
        "article_range": {
            "start_index": start_index,
            "article_count": article_count
        },
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "methods": {
            "static_request": static_result,
            "api_request": api_result,
            "dynamic_render": dynamic_result
        },
        "recommendation": None
    }
    
    if dynamic_result:
        results["recommendation"] = "动态渲染方法成功，建议使用Selenium进行后续爬取"
    elif api_result:
        results["recommendation"] = "API请求方法成功，建议直接调用API进行后续爬取"
    elif static_result:
        results["recommendation"] = "仅静态请求成功，但可能无法获取完整内容，建议改用动态渲染方法"
    else:
        results["recommendation"] = "所有方法均失败，建议检查网络连接或尝试其他策略"
    
    results_path = os.path.join(output_dir, "debug_summary.json")
    with open(results_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"已保存调试汇总到 {results_path}")
    
    logger.info("=" * 50)
    logger.info(f"调试完成: {results['recommendation']}")
    logger.info("=" * 50)

@dataclass
class ArticleInfo:
    """文章信息数据类"""
    index: int
    title: str
    url: str
    author: str
    content_hash: str
    timestamp: str
    text: str
    
class ArticleMonitor:
    """重構的文章監控和管理類 - 不依賴articles.jsonl"""
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.seen_hashes: Set[str] = set()
        self.total_articles = 0
        self.saved_articles = 0
        self.duplicate_articles = 0
        self.lock = Lock()
        self.stats_file = self.output_dir / "scraping_stats.json"
        self.urls_json_file = self.output_dir / "collected_urls.json"
        
        # 初始化分片管理器
        self.shard_manager = URLShardManager(output_dir)
        
        # 從collected_urls.json載入已知雜湊值
        self._load_existing_hashes()
        
    def _load_existing_hashes(self):
        """從collected_urls.json或分片檔案載入雜湊值，避免重複爬取"""
        try:
            # 檢查主檔案是否已分片
            if os.path.exists(self.urls_json_file):
                with open(self.urls_json_file, "r", encoding="utf-8") as f:
                    try:
                        urls_data = json.load(f)
                        # 檢查是否已分片
                        if urls_data.get("is_sharded", False):
                            logger.info("URL檔案已分片，從分片中載入雜湊值")
                            self._load_hashes_from_shards()
                            return
                    except json.JSONDecodeError:
                        logger.error(f"JSON解析錯誤: {self.urls_json_file}")
            
            # 未分片或檔案不存在，繼續嘗試從單一檔案載入
            if os.path.exists(self.urls_json_file):
                with open(self.urls_json_file, "r", encoding="utf-8") as f:
                    try:
                        urls_data = json.load(f)
                        if "urls" in urls_data and isinstance(urls_data["urls"], list):
                            for url_entry in urls_data["urls"]:
                                if "content_hash" in url_entry and url_entry["content_hash"]:
                                    self.seen_hashes.add(url_entry["content_hash"])
                                    if url_entry.get("processed", False) and url_entry.get("success", False):
                                        self.saved_articles += 1
                        # 載入總計數據
                        if "metadata" in urls_data:
                            self.total_articles = urls_data["metadata"].get("total_processed", 0)
                            self.duplicate_articles = urls_data["metadata"].get("duplicate_count", 0)
                    except json.JSONDecodeError:
                        logger.error(f"JSON解析錯誤: {self.urls_json_file}")
                logger.info(f"從collected_urls.json載入了 {len(self.seen_hashes)} 個雜湊值")
        except Exception as e:
            logger.error(f"載入現有雜湊值時出錯: {e}")
            
    def _load_hashes_from_shards(self):
        """從分片中載入雜湊值"""
        try:
            # 獲取分片索引
            index_file = os.path.join(self.output_dir, "url_shard_index.json")
            if not os.path.exists(index_file):
                logger.error("分片索引檔案不存在")
                return
                
            with open(index_file, "r", encoding="utf-8") as f:
                index_data = json.load(f)
                
            # 從每個分片載入雜湊
            hash_count = 0
            processed_count = 0
            
            for shard_info in index_data.get("shards", []):
                if "filename" in shard_info:
                    shard_file = os.path.join(self.output_dir, shard_info["filename"])
                    if os.path.exists(shard_file):
                        try:
                            with open(shard_file, "r", encoding="utf-8") as f:
                                shard_data = json.load(f)
                                if "urls" in shard_data:
                                    for url_entry in shard_data["urls"]:
                                        if "content_hash" in url_entry and url_entry["content_hash"]:
                                            self.seen_hashes.add(url_entry["content_hash"])
                                            hash_count += 1
                                            if url_entry.get("processed", False) and url_entry.get("success", False):
                                                processed_count += 1
                        except Exception as e:
                            logger.error(f"讀取分片檔案出錯: {shard_file}, {e}")
            
            # 更新計數
            self.saved_articles = processed_count
            
            # 從分片索引或主檔案更新總計數
            self.total_articles = index_data.get("processed_count", processed_count)
            self.duplicate_articles = index_data.get("duplicate_count", 0)
            
            logger.info(f"從分片中載入了 {hash_count} 個雜湊值，其中 {processed_count} 個已處理成功")
            
        except Exception as e:
            logger.error(f"從分片載入雜湊值時出錯: {e}")
        
    def check_duplicate(self, content_hash: str) -> bool:
        """檢查文章是否重複，返回是否為新文章"""
        with self.lock:
            self.total_articles += 1
            if content_hash in self.seen_hashes:
                self.duplicate_articles += 1
                logger.debug(f"發現重複文章雜湊: {content_hash[:8]}...")
                return False
            
            self.seen_hashes.add(content_hash)
            self.saved_articles += 1
            logger.debug(f"新文章雜湊: {content_hash[:8]}...")
            return True
            
    def update_stats(self) -> None:
        """更新統計檔案，不依賴articles.jsonl"""
        stats = {
            "timestamp": datetime.now().isoformat(),
            "total_articles": self.total_articles,
            "saved_articles": self.saved_articles,
            "duplicate_articles": self.duplicate_articles,
            "unique_ratio": round(self.saved_articles / max(self.total_articles, 1) * 100, 2)
        }
        
        # 檢查是否使用分片
        if os.path.exists(os.path.join(self.output_dir, "url_shard_index.json")):
            # 從分片索引獲取更多詳細資訊
            try:
                with open(os.path.join(self.output_dir, "url_shard_index.json"), "r", encoding="utf-8") as f:
                    index_data = json.load(f)
                    stats.update({
                        "is_sharded": True,
                        "total_shards": index_data.get("total_shards", 0),
                        "total_urls": index_data.get("total_urls", 0),
                        "shard_info": {
                            "shard_size": index_data.get("shard_size", 0),
                            "last_updated": index_data.get("last_updated", "")
                        }
                    })
            except Exception as e:
                logger.error(f"讀取分片索引時出錯: {e}")
        
        try:
            # 確保目錄存在
            os.makedirs(os.path.dirname(self.stats_file), exist_ok=True)
            with open(self.stats_file, "w", encoding="utf-8") as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"更新統計信息時出錯: {e}")
            
    def add_or_update_url(self, url: str, status_data: dict) -> bool:
        """添加或更新URL狀態，同時更新統計資訊"""
        try:
            # 使用分片管理器更新URL
            if "content_hash" in status_data and status_data["content_hash"]:
                # 添加雜湊到記憶中集合
                self.seen_hashes.add(status_data["content_hash"])
                
            # 更新URL狀態
            success = self.shard_manager.update_url(url, status_data)
            if not success:
                # 嘗試添加新URL
                self.shard_manager.add_url(url, {"url": url, **status_data})
                
            # 更新分片索引中的統計資訊
            self._update_stats_in_index()
                
            return True
        except Exception as e:
            logger.error(f"添加或更新URL時出錯: {url}, {e}")
            return False
            
    def _update_stats_in_index(self):
        """更新分片索引中的統計資訊"""
        try:
            index_file = os.path.join(self.output_dir, "url_shard_index.json")
            if os.path.exists(index_file):
                with open(index_file, "r", encoding="utf-8") as f:
                    index_data = json.load(f)
                    
                # 更新統計資訊
                index_data.update({
                    "last_updated": datetime.now().isoformat(),
                    "processed_count": self.saved_articles,
                    "duplicate_count": self.duplicate_articles,
                    "total_processed": self.total_articles
                })
                
                with open(index_file, "w", encoding="utf-8") as f:
                    json.dump(index_data, f, ensure_ascii=False, indent=2)
                    
        except Exception as e:
            logger.error(f"更新分片索引統計資訊時出錯: {e}")

class AsyncArticleFetcher:
    """異步文章獲取器 - 重構版本，不再依賴articles.jsonl"""
    def __init__(self, monitor: ArticleMonitor, max_workers: int = 5):
        self.monitor = monitor
        self.max_workers = max_workers
        self.session = None
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.processed_count = 0
        self.success_count = 0
        self.failed_count = 0
        self.duplicate_count = 0
        self.lock = Lock()
        
    async def create_session(self):
        """創建異步HTTP會話"""
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=60, connect=30, sock_connect=30, sock_read=30)
            self.session = aiohttp.ClientSession(headers=generate_headers(), timeout=timeout)
            
    async def close_session(self):
        """關閉異步HTTP會話"""
        if self.session:
            await self.session.close()
            self.session = None
            
    async def fetch_article(self, url: str, index: int) -> None:
        """異步獲取單篇文章並更新狀態 - 重構版本，不再保存到articles.jsonl"""
        with self.lock:
            self.processed_count += 1
        
        try:
            logger.info(f"開始獲取文章 {index}: {url}")
            
            # 添加重試邏輯
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    if self.session is None:
                        await self.create_session()
                    
                    async with self.session.get(url, timeout=30, allow_redirects=True) as response:
                        if response.status == 200:
                            html = await response.text()
                            soup = BeautifulSoup(html, 'html.parser')
                            text = soup.get_text(strip=True)
                            content_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                            
                            # 檢查文章是否重複
                            is_new = self.monitor.check_duplicate(content_hash)
                            
                            # 構建文章狀態數據
                            article_data = {
                                "title": soup.title.string if soup.title else "",
                                "author": soup.select_one('.follow-nickName').text if soup.select_one('.follow-nickName') else "",
                                "content_hash": content_hash,
                                "processed": True,
                                "success": True,
                                "processed_at": datetime.now().isoformat(),
                                "text_sample": text[:500] if is_new else ""  # 只在新文章時保存摘要
                            }
                            
                            # 更新URL狀態
                            await self.update_url_status(url, article_data)
                            
                            if is_new:
                                with self.lock:
                                    self.success_count += 1
                                logger.info(f"成功獲取文章 {index}: {url} (總成功: {self.success_count})")
                            else:
                                with self.lock:
                                    self.duplicate_count += 1
                                logger.info(f"文章 {index}: {url} 是重複文章，已跳過 (總重複: {self.duplicate_count})")
                            break  # 成功獲取，跳出重試循環
                        else:
                            logger.warning(f"獲取文章失敗 {index}: {url}, 狀態碼: {response.status}, 重試 {retry_count+1}/{max_retries}")
                            retry_count += 1
                            if retry_count >= max_retries:
                                with self.lock:
                                    self.failed_count += 1
                                # 更新失敗狀態
                                await self.update_url_status(url, {
                                    "processed": True,
                                    "success": False,
                                    "error": f"HTTP錯誤: {response.status}",
                                    "processed_at": datetime.now().isoformat()
                                })
                                logger.warning(f"獲取文章最終失敗 {index}: {url}, 狀態碼: {response.status} (總失敗: {self.failed_count})")
                            await asyncio.sleep(2)  # 等待一段時間再重試
                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    retry_count += 1
                    logger.error(f"獲取文章網絡錯誤 {index}: {url}, 錯誤: {e}, 重試 {retry_count}/{max_retries}")
                    if retry_count >= max_retries:
                        with self.lock:
                            self.failed_count += 1
                        # 更新失敗狀態
                        await self.update_url_status(url, {
                            "processed": True,
                            "success": False,
                            "error": f"網絡錯誤: {str(e)}",
                            "processed_at": datetime.now().isoformat()
                        })
                        logger.error(f"獲取文章最終失敗 {index}: {url}, 錯誤: {e} (總失敗: {self.failed_count})")
                    await asyncio.sleep(2)  # 等待一段時間再重試
                    
                    # 如果會話出錯，嘗試重新創建會話
                    if self.session is not None:
                        try:
                            await self.session.close()
                        except:
                            pass
                        self.session = None
                        await self.create_session()
        except Exception as e:
            with self.lock:
                self.failed_count += 1
            # 更新錯誤狀態
            await self.update_url_status(url, {
                "processed": True,
                "success": False,
                "error": f"處理錯誤: {str(e)}",
                "processed_at": datetime.now().isoformat()
            })
            logger.error(f"獲取文章出錯 {index}: {url}, 錯誤: {e} (總失敗: {self.failed_count})")
    
    async def update_url_status(self, url: str, status_data: dict) -> None:
        """更新URL狀態到collected_urls.json或分片中"""
        # 使用線程池執行IO操作，避免阻塞異步循環
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                self.executor,
                self.monitor.add_or_update_url,
                url,
                status_data
            )
            
            # 定期更新統計數據
            if random.random() < 0.1:  # 約10%的機率更新統計
                await loop.run_in_executor(
                    self.executor,
                    self.monitor.update_stats
                )
                
        except Exception as e:
            logger.error(f"更新URL狀態失敗: {url}, 錯誤: {e}")
            
    async def process_urls(self, urls: List[str]) -> None:
        """處理多個URL"""
        logger.info(f"開始處理 {len(urls)} 個URL")
        try:
            await self.create_session()
            tasks = []
            for i, url in enumerate(urls):
                tasks.append(self.fetch_article(url, i))
                if len(tasks) >= self.max_workers:
                    await asyncio.gather(*tasks)
                    tasks = []
                    # 短暫暫停，避免請求過於頻繁
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    # 輸出進度
                    logger.info(f"進度: {i+1}/{len(urls)} URLs, 成功: {self.success_count}, 重複: {self.duplicate_count}, 失敗: {self.failed_count}")
                    
                    # 每處理50個URL更新一次統計
                    if i % 50 == 0:
                        await asyncio.to_thread(self.monitor.update_stats)
                        
            if tasks:
                await asyncio.gather(*tasks)
                
            # 更新最終統計
            await asyncio.to_thread(self.monitor.update_stats)
            
            logger.info(f"URL處理完成: 總計 {len(urls)} URLs, 成功: {self.success_count}, 重複: {self.duplicate_count}, 失敗: {self.failed_count}")
        except Exception as e:
            logger.error(f"處理URL時出錯: {e}")
        finally:
            await self.close_session()

# 从 JSON 和文本备份中恢复 URL
def recover_urls_from_backups(url_backup_file: str, url_backup_file_json: str, article_urls: List[str], url_discovery_times: Dict[str, str], query: str) -> bool:
    """
    嘗試從 JSON 和文本備份中恢復 URL 列表，保持向後兼容性。
    
    Args:
        url_backup_file: 文本備份文件路徑
        url_backup_file_json: JSON 備份文件路徑
        article_urls: 要填充的文章 URL 列表
        url_discovery_times: URL 發現時間字典
        query: 查詢關鍵詞
        
    Returns:
        是否成功恢復 URL
    """
    # 首先嘗試從 JSON 備份恢復
    json_success = load_urls_from_backup_json(url_backup_file_json, article_urls, url_discovery_times)
    
    # 如果 JSON 恢復失敗或未找到，嘗試從文本備份恢復
    if not json_success:
        text_success = load_urls_from_backup(url_backup_file, article_urls)
        if not text_success:
            return False
    
    logger.info(f"從備份文件恢復了 {len(article_urls)} 個 URL")
    return True

def load_urls_from_backup_json(url_backup_file_json: str, article_urls: List[str], url_discovery_times: Dict[str, str]) -> bool:
    """
    從 JSON 備份文件加載 URL
    
    Args:
        url_backup_file_json: JSON 備份文件路徑
        article_urls: 要填充的文章 URL 列表
        url_discovery_times: URL 發現時間字典
        
    Returns:
        是否成功加載 URL
    """
    try:
        if os.path.exists(url_backup_file_json):
            with open(url_backup_file_json, "r", encoding="utf-8") as f:
                backup_data = json.load(f)
                
            # 檢查是否有壓縮備份
            compressed_file = url_backup_file_json + ".gz"
            if not backup_data and os.path.exists(compressed_file):
                import gzip
                try:
                    with gzip.open(compressed_file, "rt", encoding="utf-8") as f:
                        backup_data = json.load(f)
                    logger.info("從壓縮 JSON 備份文件加載 URL")
                except Exception as e:
                    logger.error(f"讀取壓縮 JSON 備份失敗: {e}")
                
            # 獲取 URL 列表
            if isinstance(backup_data, dict) and "urls" in backup_data:
                # 新格式：結構化 JSON
                urls_data = backup_data["urls"]
                loaded_urls = []
                
                for item in urls_data:
                    if isinstance(item, dict) and "url" in item:
                        url = item["url"]
                        loaded_urls.append(url)
                        
                        # 恢復發現時間
                        if "discovered_at" in item:
                            url_discovery_times[url] = item["discovered_at"]
                        
                        # 恢復重試計數
                        if "retry_count" in item and item["retry_count"] > 0:
                            # 這個方法不是很理想，但可以工作
                            setattr(url, '_retry_count', item["retry_count"])
                    elif isinstance(item, str):
                        loaded_urls.append(item)
            elif isinstance(backup_data, list):
                # 兼容舊格式：直接 URL 數組
                loaded_urls = backup_data
            else:
                logger.warning("JSON 備份格式無法識別")
                return False
                
            # 過濾並添加到文章 URL 列表
            valid_urls = [url for url in loaded_urls if url and isinstance(url, str)]
            for url in valid_urls:
                if url not in article_urls:
                    article_urls.append(url)
                    
            logger.info(f"從 JSON 備份文件加載了 {len(valid_urls)} 個 URL")
            return len(valid_urls) > 0
        return False
    except Exception as e:
        logger.error(f"加載 JSON 備份文件時出錯: {e}")
        return False

def load_urls_from_backup(url_backup_file: str, article_urls: List[str]) -> bool:
    """
    從文本備份文件加載 URL（向後兼容）
    
    Args:
        url_backup_file: 文本備份文件路徑
        article_urls: 要填充的文章 URL 列表
        
    Returns:
        是否成功加載 URL
    """
    try:
        if os.path.exists(url_backup_file):
            with open(url_backup_file, "r", encoding="utf-8") as f:
                loaded_urls = [line.strip() for line in f if line.strip()]
                
            # 過濾並添加到文章 URL 列表
            valid_urls = [url for url in loaded_urls if url and isinstance(url, str)]
            for url in valid_urls:
                if url not in article_urls:
                    article_urls.append(url)
                    
            logger.info(f"從文本備份文件加載了 {len(valid_urls)} 個 URL")
            return len(valid_urls) > 0
        return False
    except Exception as e:
        logger.error(f"加載文本備份文件時出錯: {e}")
        return False

# 工具函數：用於監控操作的效能
def measure_performance(func):
    """
    裝飾器：測量函數執行的時間、記憶體使用和 CPU 使用率
    """
    def wrapper(*args, **kwargs):
        # 獲取開始時的資源使用情況
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_cpu = process.cpu_percent(interval=None)
        start_time = time.time()
        
        # 執行函數
        result = func(*args, **kwargs)
        
        # 計算資源使用
        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024  # MB
        end_cpu = process.cpu_percent(interval=None)
        
        # 計算差異
        duration = end_time - start_time
        memory_used = end_memory - start_memory
        
        # 記錄性能指標
        logger.info(f"性能指標 - {func.__name__}: 耗時 {duration:.2f}秒, "
                   f"記憶體增加: {memory_used:.2f}MB, CPU使用率: {end_cpu:.1f}%")
        
        return result
    return wrapper

# URL 備份性能測試
def benchmark_url_backup(urls_count=10000, output_dir="benchmark_results"):
    """
    測試不同 URL 備份方法的性能
    
    Args:
        urls_count: 測試 URL 的數量
        output_dir: 輸出目錄
    
    Returns:
        包含性能指標的字典
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # 生成測試 URL
    logger.info(f"生成 {urls_count} 個測試 URL")
    test_urls = [f"https://blog.csdn.net/test_article_{i}" for i in range(urls_count)]
    test_discovery_times = {url: datetime.now().isoformat() for url in test_urls}
    
    results = {}
    
    # 測試文本備份
    start_time = time.time()
    text_file = os.path.join(output_dir, "benchmark_text.txt")
    with open(text_file, "w", encoding="utf-8") as f:
        for url in test_urls:
            f.write(f"{url}\n")
    text_write_time = time.time() - start_time
    text_file_size = os.path.getsize(text_file)
    
    results["text_format"] = {
        "write_time_ms": text_write_time * 1000,
        "file_size_bytes": text_file_size,
        "bytes_per_url": text_file_size / urls_count
    }
    
    # 測試 JSON 備份
    start_time = time.time()
    json_file = os.path.join(output_dir, "benchmark_json.json")
    backup_data = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "total_urls": len(test_urls),
            "query": "benchmark",
            "version": "1.0"
        },
        "urls": [
            {
                "url": url,
                "discovered_at": test_discovery_times[url],
                "processed": False,
                "retry_count": 0
            } for url in test_urls
        ]
    }
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2)
    json_write_time = time.time() - start_time
    json_file_size = os.path.getsize(json_file)
    
    results["json_format"] = {
        "write_time_ms": json_write_time * 1000,
        "file_size_bytes": json_file_size,
        "bytes_per_url": json_file_size / urls_count
    }
    
    # 測試壓縮 JSON
    start_time = time.time()
    compressed_file = os.path.join(output_dir, "benchmark_json.json.gz")
    json_str = json.dumps(backup_data, ensure_ascii=False)
    with gzip.open(compressed_file, "wt", encoding="utf-8") as f:
        f.write(json_str)
    compressed_write_time = time.time() - start_time
    compressed_file_size = os.path.getsize(compressed_file)
    
    results["compressed_json"] = {
        "write_time_ms": compressed_write_time * 1000,
        "file_size_bytes": compressed_file_size,
        "bytes_per_url": compressed_file_size / urls_count
    }
    
    # 測試增量 JSON 備份
    start_time = time.time()
    incremental_file = os.path.join(output_dir, "benchmark_incremental.json")
    
    # 先寫入一半 URL
    half_urls = test_urls[:urls_count//2]
    half_backup_data = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "total_urls": len(half_urls),
            "query": "benchmark",
            "version": "1.0"
        },
        "urls": [
            {
                "url": url,
                "discovered_at": test_discovery_times[url],
                "processed": False,
                "retry_count": 0
            } for url in half_urls
        ]
    }
    with open(incremental_file, "w", encoding="utf-8") as f:
        json.dump(half_backup_data, f, ensure_ascii=False)
    
    # 然後增量添加另一半
    remaining_urls = test_urls[urls_count//2:]
    with open(incremental_file, "r", encoding="utf-8") as f:
        existing_data = json.load(f)
    
    existing_data["metadata"]["total_urls"] = urls_count
    existing_data["urls"].extend([
        {
            "url": url,
            "discovered_at": test_discovery_times[url],
            "processed": False,
            "retry_count": 0
        } for url in remaining_urls
    ])
    
    with open(incremental_file, "w", encoding="utf-8") as f:
        json.dump(existing_data, f, ensure_ascii=False)
    
    incremental_write_time = time.time() - start_time
    incremental_file_size = os.path.getsize(incremental_file)
    
    results["incremental_json"] = {
        "write_time_ms": incremental_write_time * 1000,
        "file_size_bytes": incremental_file_size,
        "bytes_per_url": incremental_file_size / urls_count
    }
    
    # 測試讀取性能
    # 文本讀取
    start_time = time.time()
    loaded_urls = []
    with open(text_file, "r", encoding="utf-8") as f:
        for line in f:
            loaded_urls.append(line.strip())
    text_read_time = time.time() - start_time
    results["text_format"]["read_time_ms"] = text_read_time * 1000
    
    # JSON 讀取
    start_time = time.time()
    with open(json_file, "r", encoding="utf-8") as f:
        loaded_data = json.load(f)
    json_read_time = time.time() - start_time
    results["json_format"]["read_time_ms"] = json_read_time * 1000
    
    # 壓縮 JSON 讀取
    start_time = time.time()
    with gzip.open(compressed_file, "rt", encoding="utf-8") as f:
        loaded_compressed_data = json.load(f)
    compressed_read_time = time.time() - start_time
    results["compressed_json"]["read_time_ms"] = compressed_read_time * 1000
    
    # 保存結果
    results_file = os.path.join(output_dir, "benchmark_results.json")
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "urls_count": urls_count,
            "results": results
        }, f, ensure_ascii=False, indent=2)
    
    logger.info(f"性能測試結果已保存到 {results_file}")
    return results

# 使用性能監控裝飾器優化備份函數
@measure_performance
def save_urls_to_backup_json():
    try:
        # 構建備份數據結構
        backup_data = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "total_urls": len(article_urls),
                "query": query,
                "version": "1.0",
                "scraping_stats": {
                    "processed": len(processed_urls),
                    "success_count": fetcher.success_count,
                    "duplicate_count": fetcher.duplicate_count,
                    "failed_count": fetcher.failed_count
                }
            },
            "urls": [
                {
                    "url": url,
                    "discovered_at": url_discovery_times.get(url, datetime.now().isoformat()),
                    "processed": url in processed_urls,
                    "retry_count": getattr(url, '_retry_count', 0) if hasattr(url, '_retry_count') else 0
                } for url in article_urls
            ]
        }
        
        # 使用JSON格式寫入文件
        with open(url_backup_file_json, "w", encoding="utf-8") as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"已將 {len(article_urls)} 個URL以JSON格式保存到備份文件")
    except Exception as e:
        logger.error(f"保存URL到JSON備份文件時出錯: {e}")
        # 嘗試使用文本格式作為後備方案
        save_urls_to_backup()

class URLShardManager:
    """URL分片管理器 - 處理大型collected_urls.json檔案"""
    def __init__(self, output_dir: str, shard_size: int = 5000):
        self.output_dir = Path(output_dir)
        self.shard_size = shard_size  # 每個分片的URL數量
        self.current_shard = 0
        self.shard_map = {}  # URL -> 分片編號
        self.lock = Lock()
        
        # 初始化：掃描現有分片
        self._scan_existing_shards()
        
    def _scan_existing_shards(self):
        """掃描目錄中現有的分片檔案"""
        shard_pattern = os.path.join(self.output_dir, "collected_urls_shard_*.json")
        shard_files = glob.glob(shard_pattern)
        
        if not shard_files:
            # 檢查是否有未分片的主檔案
            main_file = os.path.join(self.output_dir, "collected_urls.json")
            if os.path.exists(main_file):
                # 如果主檔案存在且需要分片，執行初始分片
                try:
                    with open(main_file, "r", encoding="utf-8") as f:
                        urls_data = json.load(f)
                        
                    if "urls" in urls_data and len(urls_data["urls"]) > self.shard_size:
                        self._perform_initial_sharding(urls_data)
                    else:
                        self.current_shard = 0
                except Exception as e:
                    logger.error(f"讀取主URL檔案失敗: {e}")
                    self.current_shard = 0
            else:
                self.current_shard = 0
            return
            
        # 從現有分片檔案名稱獲取最大分片編號
        shard_numbers = []
        for shard_file in shard_files:
            match = re.search(r"collected_urls_shard_(\d+)\.json", shard_file)
            if match:
                shard_numbers.append(int(match.group(1)))
                
        if shard_numbers:
            self.current_shard = max(shard_numbers)
            logger.info(f"發現 {len(shard_numbers)} 個URL分片，當前分片: {self.current_shard}")
            
            # 建立URL到分片的映射
            for shard_num in range(self.current_shard + 1):
                shard_file = os.path.join(self.output_dir, f"collected_urls_shard_{shard_num}.json")
                if os.path.exists(shard_file):
                    try:
                        with open(shard_file, "r", encoding="utf-8") as f:
                            shard_data = json.load(f)
                            if "urls" in shard_data:
                                for url_entry in shard_data["urls"]:
                                    if "url" in url_entry:
                                        self.shard_map[url_entry["url"]] = shard_num
                    except Exception as e:
                        logger.error(f"讀取分片 {shard_num} 失敗: {e}")
    
    def _perform_initial_sharding(self, urls_data):
        """執行初始分片操作"""
        if "urls" not in urls_data or not urls_data["urls"]:
            return
            
        all_urls = urls_data["urls"]
        total_urls = len(all_urls)
        shard_count = (total_urls + self.shard_size - 1) // self.shard_size  # 向上取整
        
        logger.info(f"執行初始URL分片: {total_urls} URLs 分成 {shard_count} 個分片")
        
        for shard_num in range(shard_count):
            start_idx = shard_num * self.shard_size
            end_idx = min((shard_num + 1) * self.shard_size, total_urls)
            
            # 創建分片數據
            shard_data = {
                "metadata": urls_data.get("metadata", {}).copy(),
                "shard_info": {
                    "shard_number": shard_num,
                    "total_shards": shard_count,
                    "url_count": end_idx - start_idx,
                    "start_index": start_idx,
                    "end_index": end_idx,
                    "created_at": datetime.now().isoformat()
                },
                "urls": all_urls[start_idx:end_idx]
            }
            
            # 更新分片特定元數據
            if "metadata" in shard_data:
                shard_data["metadata"].update({
                    "is_shard": True,
                    "shard_number": shard_num,
                    "shard_created_at": datetime.now().isoformat()
                })
            
            # 保存分片
            shard_file = os.path.join(self.output_dir, f"collected_urls_shard_{shard_num}.json")
            with open(shard_file, "w", encoding="utf-8") as f:
                json.dump(shard_data, f, ensure_ascii=False, indent=2)
                
            # 更新映射
            for url_entry in shard_data["urls"]:
                if "url" in url_entry:
                    self.shard_map[url_entry["url"]] = shard_num
                    
        # 更新當前分片編號
        self.current_shard = shard_count - 1
        
        # 創建分片索引檔案
        self._create_shard_index(shard_count, total_urls)
        
        # 備份並替換原始檔案
        backup_file = os.path.join(self.output_dir, "collected_urls_original.json")
        os.rename(os.path.join(self.output_dir, "collected_urls.json"), backup_file)
        
        # 創建新的collected_urls.json作為索引
        main_file = os.path.join(self.output_dir, "collected_urls.json")
        with open(main_file, "w", encoding="utf-8") as f:
            json.dump({
                "metadata": urls_data.get("metadata", {}).copy(),
                "is_sharded": True,
                "shard_count": shard_count,
                "total_urls": total_urls,
                "sharding_date": datetime.now().isoformat(),
                "shard_index_file": "url_shard_index.json"
            }, f, ensure_ascii=False, indent=2)
            
        logger.info(f"初始分片完成：{shard_count}個分片，總計{total_urls}個URL")
    
    def _create_shard_index(self, shard_count, total_urls):
        """創建分片索引檔案"""
        index_file = os.path.join(self.output_dir, "url_shard_index.json")
        
        index_data = {
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "total_shards": shard_count,
            "total_urls": total_urls,
            "shard_size": self.shard_size,
            "shards": []
        }
        
        # 添加各分片資訊
        for shard_num in range(shard_count):
            shard_file = os.path.join(self.output_dir, f"collected_urls_shard_{shard_num}.json")
            if os.path.exists(shard_file):
                try:
                    file_size = os.path.getsize(shard_file)
                    url_count = 0
                    with open(shard_file, "r", encoding="utf-8") as f:
                        shard_data = json.load(f)
                        if "urls" in shard_data:
                            url_count = len(shard_data["urls"])
                    
                    index_data["shards"].append({
                        "shard_number": shard_num,
                        "filename": f"collected_urls_shard_{shard_num}.json",
                        "url_count": url_count,
                        "file_size_bytes": file_size,
                        "last_modified": datetime.fromtimestamp(os.path.getmtime(shard_file)).isoformat()
                    })
                except Exception as e:
                    logger.error(f"處理分片 {shard_num} 索引時出錯: {e}")
        
        # 保存索引
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"URL分片索引已創建: {index_file}")
    
    def get_url_shard(self, url: str) -> int:
        """獲取URL所在的分片編號"""
        with self.lock:
            return self.shard_map.get(url, -1)
    
    def add_url(self, url: str, url_data: dict) -> int:
        """添加新URL並分配到適當的分片"""
        with self.lock:
            # 檢查URL是否已有分片
            if url in self.shard_map:
                return self.shard_map[url]
                
            # 檢查當前分片是否已滿
            current_shard_file = os.path.join(self.output_dir, f"collected_urls_shard_{self.current_shard}.json")
            if os.path.exists(current_shard_file):
                try:
                    with open(current_shard_file, "r", encoding="utf-8") as f:
                        shard_data = json.load(f)
                        if "urls" in shard_data and len(shard_data["urls"]) >= self.shard_size:
                            # 當前分片已滿，創建新分片
                            self.current_shard += 1
                            shard_data = {"metadata": {}, "urls": []}
                except Exception as e:
                    logger.error(f"檢查分片容量時出錯: {e}")
                    shard_data = {"metadata": {}, "urls": []}
            else:
                # 分片不存在，創建新分片
                shard_data = {"metadata": {}, "urls": []}
            
            # 將URL添加到分片
            if "urls" not in shard_data:
                shard_data["urls"] = []
                
            shard_data["urls"].append(url_data)
            
            # 更新分片元數據
            if "metadata" not in shard_data:
                shard_data["metadata"] = {}
                
            shard_data["metadata"].update({
                "last_updated": datetime.now().isoformat(),
                "url_count": len(shard_data["urls"]),
                "shard_number": self.current_shard
            })
            
            # 保存更新後的分片
            with open(current_shard_file, "w", encoding="utf-8") as f:
                json.dump(shard_data, f, ensure_ascii=False, indent=2)
                
            # 更新映射
            self.shard_map[url] = self.current_shard
            
            # 更新分片索引
            self._update_shard_index()
            
            return self.current_shard
    
    def update_url(self, url: str, status_data: dict) -> bool:
        """更新URL狀態"""
        with self.lock:
            # 確定URL所在分片
            shard_num = self.get_url_shard(url)
            
            # URL尚未分配分片，先添加
            if shard_num == -1:
                self.add_url(url, {"url": url, **status_data})
                return True
                
            # 讀取分片
            shard_file = os.path.join(self.output_dir, f"collected_urls_shard_{shard_num}.json")
            if not os.path.exists(shard_file):
                logger.error(f"分片文件不存在: {shard_file}")
                return False
                
            try:
                with open(shard_file, "r", encoding="utf-8") as f:
                    shard_data = json.load(f)
                    
                # 更新URL數據
                updated = False
                if "urls" in shard_data:
                    for url_entry in shard_data["urls"]:
                        if url_entry.get("url") == url:
                            url_entry.update(status_data)
                            updated = True
                            break
                            
                if not updated:
                    logger.warning(f"URL {url} 在分片 {shard_num} 中未找到")
                    return False
                    
                # 更新分片元數據
                if "metadata" in shard_data:
                    shard_data["metadata"]["last_updated"] = datetime.now().isoformat()
                
                # 保存更新後的分片
                with open(shard_file, "w", encoding="utf-8") as f:
                    json.dump(shard_data, f, ensure_ascii=False, indent=2)
                    
                return True
                
            except Exception as e:
                logger.error(f"更新分片中的URL時出錯: {e}")
                return False
    
    def _update_shard_index(self):
        """更新分片索引"""
        index_file = os.path.join(self.output_dir, "url_shard_index.json")
        
        try:
            if os.path.exists(index_file):
                with open(index_file, "r", encoding="utf-8") as f:
                    index_data = json.load(f)
            else:
                index_data = {
                    "created_at": datetime.now().isoformat(),
                    "total_shards": self.current_shard + 1,
                    "shard_size": self.shard_size,
                    "shards": []
                }
                
            # 更新索引元數據
            index_data["last_updated"] = datetime.now().isoformat()
            index_data["total_shards"] = self.current_shard + 1
            
            # 計算總URL數
            total_urls = 0
            existing_shards = set()
            
            for shard_info in index_data.get("shards", []):
                if "shard_number" in shard_info:
                    existing_shards.add(shard_info["shard_number"])
            
            # 更新現有分片資訊
            for shard_num in range(self.current_shard + 1):
                shard_file = os.path.join(self.output_dir, f"collected_urls_shard_{shard_num}.json")
                if os.path.exists(shard_file):
                    file_size = os.path.getsize(shard_file)
                    url_count = 0
                    
                    try:
                        with open(shard_file, "r", encoding="utf-8") as f:
                            shard_data = json.load(f)
                            if "urls" in shard_data:
                                url_count = len(shard_data["urls"])
                                total_urls += url_count
                    except Exception as e:
                        logger.error(f"讀取分片 {shard_num} 統計時出錯: {e}")
                        continue
                        
                    # 更新或新增分片資訊
                    shard_info = {
                        "shard_number": shard_num,
                        "filename": f"collected_urls_shard_{shard_num}.json",
                        "url_count": url_count,
                        "file_size_bytes": file_size,
                        "last_modified": datetime.fromtimestamp(os.path.getmtime(shard_file)).isoformat()
                    }
                    
                    if shard_num in existing_shards:
                        # 更新現有記錄
                        for i, existing_info in enumerate(index_data["shards"]):
                            if existing_info.get("shard_number") == shard_num:
                                index_data["shards"][i] = shard_info
                                break
                    else:
                        # 新增記錄
                        index_data["shards"].append(shard_info)
                        existing_shards.add(shard_num)
            
            # 更新總URL數
            index_data["total_urls"] = total_urls
            
            # 保存更新後的索引
            with open(index_file, "w", encoding="utf-8") as f:
                json.dump(index_data, f, ensure_ascii=False, indent=2)
                
            # 更新主檔案引用
            main_file = os.path.join(self.output_dir, "collected_urls.json")
            main_data = {
                "is_sharded": True,
                "shard_count": self.current_shard + 1,
                "total_urls": total_urls,
                "last_updated": datetime.now().isoformat(),
                "shard_index_file": "url_shard_index.json"
            }
            
            with open(main_file, "w", encoding="utf-8") as f:
                json.dump(main_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logger.error(f"更新分片索引時出錯: {e}")

def migrate_data_from_articlesjsonl(output_dir: str) -> bool:
    """
    將articles.jsonl數據遷移至collected_urls.json或分片結構
    
    Args:
        output_dir: 輸出目錄路徑
        
    Returns:
        遷移是否成功
    """
    articles_file = os.path.join(output_dir, "articles.jsonl")
    urls_file = os.path.join(output_dir, "collected_urls.json")
    
    if not os.path.exists(articles_file):
        logger.info("無articles.jsonl檔案，無需遷移")
        return False
    
    logger.info(f"開始將數據從 {articles_file} 遷移到URL結構")
    start_time = time.time()
    
    # 載入articles.jsonl
    articles_data = []
    hash_set = set()  # 用於去重
    try:
        with open(articles_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    article = json.loads(line)
                    if "content_hash" in article and article["content_hash"] not in hash_set:
                        articles_data.append(article)
                        hash_set.add(article["content_hash"])
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        logger.error(f"載入articles.jsonl出錯: {e}")
        return False
    
    logger.info(f"已載入 {len(articles_data)} 篇文章，共 {len(hash_set)} 個唯一雜湊")
    
    # 檢查URL檔案是否已分片
    is_sharded = False
    try:
        if os.path.exists(urls_file):
            with open(urls_file, "r", encoding="utf-8") as f:
                urls_data = json.load(f)
                is_sharded = urls_data.get("is_sharded", False)
    except Exception as e:
        logger.error(f"檢查URL檔案分片狀態出錯: {e}")
    
    if is_sharded:
        # 使用分片結構處理
        logger.info("URL檔案已分片，將使用分片結構進行遷移")
        return migrate_to_sharded_structure(output_dir, articles_data)
    else:
        # 使用單一檔案處理
        logger.info("URL檔案未分片，將遷移到單一檔案")
        return migrate_to_single_file(output_dir, articles_data)

def migrate_to_single_file(output_dir: str, articles_data: List[dict]) -> bool:
    """將文章數據遷移到單一collected_urls.json檔案"""
    urls_file = os.path.join(output_dir, "collected_urls.json")
    
    # 載入現有collected_urls.json
    urls_data = {"metadata": {}, "urls": []}
    try:
        if os.path.exists(urls_file):
            with open(urls_file, "r", encoding="utf-8") as f:
                urls_data = json.load(f)
    except Exception as e:
        logger.error(f"載入collected_urls.json出錯: {e}")
        # 創建新的結構
        urls_data = {"metadata": {}, "urls": []}
    
    # 建立URL映射
    url_map = {}
    for url_entry in urls_data.get("urls", []):
        if "url" in url_entry:
            url_map[url_entry["url"]] = url_entry
    
    # 合併數據
    update_count = 0
    new_count = 0
    
    for article in articles_data:
        if "url" in article and article["url"]:
            if article["url"] in url_map:
                # 更新既有URL
                url_map[article["url"]].update({
                    "content_hash": article.get("content_hash", ""),
                    "title": article.get("title", ""),
                    "author": article.get("author", ""),
                    "processed": True,
                    "success": True,
                    "text_sample": article.get("text", "")[:500] if "text" in article else "",
                    "processed_at": article.get("timestamp", datetime.now().isoformat())
                })
                update_count += 1
            else:
                # 添加新URL
                urls_data["urls"].append({
                    "url": article["url"],
                    "content_hash": article.get("content_hash", ""),
                    "title": article.get("title", ""),
                    "author": article.get("author", ""),
                    "processed": True,
                    "success": True,
                    "text_sample": article.get("text", "")[:500] if "text" in article else "",
                    "discovered_at": article.get("timestamp", datetime.now().isoformat()),
                    "processed_at": article.get("timestamp", datetime.now().isoformat())
                })
                new_count += 1
    
    # 更新元數據
    urls_data["metadata"].update({
        "timestamp": datetime.now().isoformat(),
        "migration_date": datetime.now().isoformat(),
        "total_articles_migrated": len(articles_data),
        "total_urls_updated": update_count,
        "total_urls_added": new_count,
        "total_urls": len(urls_data.get("urls", [])),
        "total_processed": sum(1 for url in urls_data.get("urls", []) if url.get("processed", False)),
        "success_count": sum(1 for url in urls_data.get("urls", []) if url.get("processed", False) and url.get("success", False)),
        "duplicate_count": len(articles_data) - (update_count + new_count) if len(articles_data) > (update_count + new_count) else 0
    })
    
    # 保存更新後的collected_urls.json
    try:
        # 備份原檔案
        if os.path.exists(urls_file):
            backup_file = urls_file + f".bak.{int(time.time())}"
            shutil.copy2(urls_file, backup_file)
            logger.info(f"已備份原URL檔案到 {backup_file}")
        
        with open(urls_file, "w", encoding="utf-8") as f:
            json.dump(urls_data, f, ensure_ascii=False, indent=2)
        logger.info(f"數據遷移成功，更新 {update_count} 個URL，新增 {new_count} 個URL，總計 {len(urls_data.get('urls', []))} 個URL")
        
        # 如果URL數量超過閾值，建議將檔案分片
        if len(urls_data.get("urls", [])) > 5000:
            logger.info("URL數量較多，建議使用 create_url_shards() 函數將檔案分片以提高效能")
            
        return True
    except Exception as e:
        logger.error(f"保存collected_urls.json失敗: {e}")
        return False

def migrate_to_sharded_structure(output_dir: str, articles_data: List[dict]) -> bool:
    """將文章數據遷移到分片結構"""
    try:
        # 初始化分片管理器
        shard_manager = URLShardManager(output_dir)
        
        # 根據URL更新分片
        success_count = 0
        failed_count = 0
        
        # 顯示進度
        total = len(articles_data)
        progress_step = max(1, total // 20)  # 5%的進度更新
        
        for i, article in enumerate(articles_data):
            if "url" in article and article["url"]:
                try:
                    # 構建文章狀態數據
                    article_data = {
                        "content_hash": article.get("content_hash", ""),
                        "title": article.get("title", ""),
                        "author": article.get("author", ""),
                        "processed": True,
                        "success": True,
                        "text_sample": article.get("text", "")[:500] if "text" in article else "",
                        "discovered_at": article.get("timestamp", datetime.now().isoformat()),
                        "processed_at": article.get("timestamp", datetime.now().isoformat())
                    }
                    
                    # 更新URL狀態
                    success = shard_manager.update_url(article["url"], article_data)
                    if not success:
                        # 嘗試添加新URL
                        shard_manager.add_url(article["url"], {"url": article["url"], **article_data})
                    
                    success_count += 1
                except Exception as e:
                    logger.error(f"更新URL分片時出錯: {article['url']}, {e}")
                    failed_count += 1
            
            # 顯示進度
            if (i + 1) % progress_step == 0 or i + 1 == total:
                progress_percent = (i + 1) / total * 100
                logger.info(f"遷移進度: {i+1}/{total} ({progress_percent:.1f}%), 成功: {success_count}, 失敗: {failed_count}")
        
        # 更新分片索引中的統計資訊
        index_file = os.path.join(output_dir, "url_shard_index.json")
        if os.path.exists(index_file):
            try:
                with open(index_file, "r", encoding="utf-8") as f:
                    index_data = json.load(f)
                
                # 更新統計資訊
                index_data.update({
                    "last_updated": datetime.now().isoformat(),
                    "migration_date": datetime.now().isoformat(),
                    "total_articles_migrated": len(articles_data),
                    "migration_success_count": success_count,
                    "migration_failed_count": failed_count
                })
                
                with open(index_file, "w", encoding="utf-8") as f:
                    json.dump(index_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"更新分片索引統計資訊時出錯: {e}")
        
        logger.info(f"分片結構遷移完成: 處理 {total} 篇文章, 成功 {success_count}, 失敗 {failed_count}")
        return success_count > 0
        
    except Exception as e:
        logger.error(f"遷移到分片結構時出錯: {e}")
        return False

def create_url_shards(output_dir: str, shard_size: int = 5000) -> bool:
    """
    將單一collected_urls.json檔案轉換為分片結構
    
    Args:
        output_dir: 輸出目錄路徑
        shard_size: 每個分片的URL數量
        
    Returns:
        分片是否成功
    """
    urls_file = os.path.join(output_dir, "collected_urls.json")
    
    if not os.path.exists(urls_file):
        logger.error("collected_urls.json不存在，無法進行分片")
        return False
    
    try:
        # 初始化分片管理器 (會自動進行分片檢測與處理)
        shard_manager = URLShardManager(output_dir, shard_size)
        
        # 檢查是否已分片
        index_file = os.path.join(output_dir, "url_shard_index.json")
        if os.path.exists(index_file):
            logger.info("URL檔案已經是分片結構，無需再次分片")
            return True
            
        # 分片管理器初始化時會自動檢測並執行分片
        # 檢查分片是否已創建
        shard_files = glob.glob(os.path.join(output_dir, "collected_urls_shard_*.json"))
        if shard_files:
            logger.info(f"分片成功: 建立了 {len(shard_files)} 個分片檔案")
            return True
        else:
            logger.warning("分片過程未建立任何分片檔案，可能是URL數量不足")
            return False
            
    except Exception as e:
        logger.error(f"創建URL分片結構時出錯: {e}")
        return False

# 刪除articles.jsonl文件的工具函數
def remove_articles_jsonl(output_dir: str, confirm: bool = False) -> bool:
    """
    當確認遷移成功後，刪除articles.jsonl檔案
    
    Args:
        output_dir: 輸出目錄路徑
        confirm: 是否確認刪除，不確認則只備份
        
    Returns:
        操作是否成功
    """
    articles_file = os.path.join(output_dir, "articles.jsonl")
    
    if not os.path.exists(articles_file):
        logger.info("未找到articles.jsonl檔案，無需刪除")
        return True
    
    # 檢查是否有遷移記錄
    urls_file = os.path.join(output_dir, "collected_urls.json")
    migration_performed = False
    
    try:
        if os.path.exists(urls_file):
            with open(urls_file, "r", encoding="utf-8") as f:
                urls_data = json.load(f)
                migration_performed = "migration_date" in urls_data.get("metadata", {})
    except Exception:
        pass
    
    if not migration_performed:
        index_file = os.path.join(output_dir, "url_shard_index.json")
        try:
            if os.path.exists(index_file):
                with open(index_file, "r", encoding="utf-8") as f:
                    index_data = json.load(f)
                    migration_performed = "migration_date" in index_data
        except Exception:
            pass
    
    if not migration_performed and not confirm:
        logger.warning("未檢測到遷移記錄，且未強制確認，操作取消")
        return False
    
    try:
        # 備份原檔案
        backup_file = articles_file + f".bak.{int(time.time())}"
        shutil.copy2(articles_file, backup_file)
        logger.info(f"已備份原articles.jsonl到 {backup_file}")
        
        if confirm:
            # 刪除articles.jsonl
            os.remove(articles_file)
            logger.info(f"已刪除 {articles_file}")
            return True
        else:
            logger.info(f"由於未確認，僅備份 {articles_file}，未刪除")
            return True
    except Exception as e:
        logger.error(f"處理articles.jsonl時出錯: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSDN 搜索調試工具")
    parser.add_argument("--query", default="ip連線", help="搜索查詢詞")
    parser.add_argument("--output-dir", default="debug_output", help="調試輸出目錄")
    parser.add_argument("--method", choices=["all", "static", "dynamic", "api"], default="dynamic", help="調試方法")
    parser.add_argument("--start", type=int, default=0, help="開始爬取的文章索引（從0開始）")
    parser.add_argument("--count", type=int, default=1000, help="要爬取的文章數量，不指定則爬取所有可獲取的文章")
    parser.add_argument("--workers", type=int, default=5, help="異步爬取的最大並發數")
    parser.add_argument("--monitor-interval", type=int, default=60, help="監控統計信息更新間隔（秒）")
    parser.add_argument("--max-pages", type=int, default=20, help="最大嘗試的頁面數量")
    parser.add_argument("--backup-format", choices=["text", "json", "json_compressed", "all"], default="json", 
                      help="URL 備份格式: text=文本, json=JSON, json_compressed=壓縮JSON, all=所有格式")
    parser.add_argument("--benchmark", action="store_true", help="執行 URL 備份性能測試")
    parser.add_argument("--benchmark-count", type=int, default=10000, help="性能測試的 URL 數量")
    parser.add_argument("--url-timeout", type=int, default=30, help="URL處理超時時間（分鐘），預設30分鐘")
    parser.add_argument("--resume", action="store_true", help="自動繼續處理未完成的URL")
    parser.add_argument("--no-resume", dest="resume", action="store_false", help="不自動繼續處理未完成的URL")
    parser.set_defaults(resume=True)
    
    # 新增重構相關命令行選項
    parser.add_argument("--migrate", action="store_true", help="將articles.jsonl數據遷移到URL結構")
    parser.add_argument("--create-shards", action="store_true", help="將單一URL檔案轉換為分片結構")
    parser.add_argument("--shard-size", type=int, default=5000, help="URL分片大小，預設5000個URL/分片")
    parser.add_argument("--remove-articles-jsonl", action="store_true", help="刪除articles.jsonl檔案（確認遷移成功後使用）")
    parser.add_argument("--force-delete", action="store_true", help="強制刪除articles.jsonl，不檢查遷移記錄")
    
    args = parser.parse_args()
    
    # 確保輸出目錄存在
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 處理重構相關操作，優先處理這些操作
    if args.migrate:
        logger.info("執行數據遷移操作")
        migrate_data_from_articlesjsonl(args.output_dir)
    
    if args.create_shards:
        logger.info(f"創建URL分片，每個分片 {args.shard_size} 個URL")
        create_url_shards(args.output_dir, args.shard_size)
    
    if args.remove_articles_jsonl:
        logger.info("處理articles.jsonl檔案")
        remove_articles_jsonl(args.output_dir, args.force_delete)
    
    # 如果只進行重構相關操作，則不執行爬蟲
    if args.migrate or args.create_shards or args.remove_articles_jsonl:
        logger.info("執行重構相關操作完成，程式結束")
        sys.exit(0)
    
    # 檢查是否運行性能測試
    if args.benchmark:
        logger.info(f"執行 URL 備份性能測試，URL 數量: {args.benchmark_count}")
        benchmark_results = benchmark_url_backup(args.benchmark_count, args.output_dir)
        
        # 顯示摘要結果
        logger.info("性能測試結果摘要:")
        for format_name, metrics in benchmark_results.items():
            logger.info(f"  {format_name}:")
            logger.info(f"    寫入時間: {metrics['write_time_ms']:.2f} ms")
            logger.info(f"    讀取時間: {metrics['read_time_ms']:.2f} ms")
            logger.info(f"    文件大小: {metrics['file_size_bytes'] / 1024:.2f} KB")
            logger.info(f"    每URL佔用: {metrics['bytes_per_url']:.2f} bytes")
        
        sys.exit(0)
    
    # 創建監控器實例 - 使用重構後的類
    monitor = ArticleMonitor(args.output_dir)
    
    # 啟動監控統計信息更新的後台線程
    def update_stats_periodically():
        while True:
            time.sleep(args.monitor_interval)
            try:
                monitor.update_stats()
                
                # 讀取scraping_stats.json中的統計數據
                stats_file = os.path.join(args.output_dir, "scraping_stats.json")
                if os.path.exists(stats_file):
                    try:
                        with open(stats_file, "r", encoding="utf-8") as f:
                            stats_data = json.load(f)
                            
                            # 顯示統計信息
                            logger.info("=" * 50)
                            logger.info(f"爬蟲統計更新:")
                            
                            # 顯示分片資訊（如果有）
                            if stats_data.get("is_sharded", False):
                                logger.info(f"URL分片信息: {stats_data.get('total_shards', 0)} 個分片, "
                                         f"共 {stats_data.get('total_urls', 0)} 個URL")
                            
                            logger.info(f"文章統計: 總處理 {stats_data.get('total_articles', 0)}, "
                                     f"已保存 {stats_data.get('saved_articles', 0)}, "
                                     f"重複 {stats_data.get('duplicate_articles', 0)}, "
                                     f"唯一率 {stats_data.get('unique_ratio', 0)}%")
                            logger.info("=" * 50)
                    except Exception as e:
                        logger.error(f"讀取統計文件時出錯: {e}")
                        # 回退到原始日誌格式
                        logger.info(f"統計更新: 總計 {monitor.total_articles} 篇文章, "
                                  f"保存 {monitor.saved_articles} 篇唯一文章, "
                                  f"發現 {monitor.duplicate_articles} 篇重複文章")
                else:
                    # 如果統計文件不存在，使用原始格式
                    logger.info(f"統計更新: 總計 {monitor.total_articles} 篇文章, "
                              f"保存 {monitor.saved_articles} 篇唯一文章, "
                              f"發現 {monitor.duplicate_articles} 篇重複文章")
            except Exception as e:
                logger.error(f"定期更新統計信息時出錯: {e}")
    
    if args.monitor_interval > 0:
        import threading
        stats_thread = threading.Thread(target=update_stats_periodically, daemon=True)
        stats_thread.start()
    
    try:
        if args.method == "all":
            debug_csdn_search(args.query, args.output_dir, args.start, args.count)
        elif args.method == "static":
            logger.error("靜態請求方法未實現。")
        elif args.method == "dynamic":
            debug_csdn_search_dynamic(args.query, args.output_dir, args.start, args.count)
        elif args.method == "api": 
            debug_csdn_search_api(args.query, args.output_dir, args.start, args.count)
        
        # 更新最終統計
        monitor.update_stats()
        
    except KeyboardInterrupt:
        logger.info("接收到中斷信號，正在保存當前進度...")
        monitor.update_stats()
        logger.info("進度已保存")
    except Exception as e:
        logger.error(f"程序執行出錯: {e}", exc_info=True)
    finally:
        logger.info("調試完成")
