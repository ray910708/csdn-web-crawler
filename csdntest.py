#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
改进版的 CSDN 搜索爬虫 - 处理SPA架构
支持选择爬取文章的起始位置和数量，并实现实时保存 JSON 数据和文章查重
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
from typing import Dict

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
        logging.FileHandler("csdn_debug.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("csdn_debug_scraper")

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
    """
    if not SELENIUM_AVAILABLE:
        logger.error("无法使用动态请求方式，因为Selenium未安装")
        return False
    
    logger.info(f"开始使用动态请求方式调试 CSDN 搜索: {query} [文章范围: {start_index} 起始, 数量 {article_count if article_count else '全部'}]")
    
    os.makedirs(output_dir, exist_ok=True)
    realtime_file = os.path.join(output_dir, "articles_realtime.jsonl")
    
    # 用于记录去重的文章哈希
    seen_hashes = set()
    total_saved = 0
    
    try:
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument(f'user-agent={generate_headers()["User-Agent"]}')
        
        driver = webdriver.Chrome(options=options)
        try:
            encoded_query = quote(query)
            search_url = f"https://so.csdn.net/so/search/s.do?q={encoded_query}&t=all"
            logger.info(f"使用Selenium请求 URL: {search_url}")
            driver.get(search_url)
            time.sleep(5)  # 初始等待
            
            debug_html_path = os.path.join(output_dir, "initial_page.html")
            with open(debug_html_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            logger.info(f"已保存初始页面源码到 {debug_html_path}")
            
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
                    logger.info(f"尝试选择器: {selector}")
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        logger.info(f"使用选择器 {selector} 找到 {len(elements)} 个元素")
                        if any(element.is_displayed() for element in elements):
                            valid_selector = selector
                            logger.info(f"找到有效选择器：{valid_selector}")
                            break
                        else:
                            logger.info(f"选择器 {selector} 找到的元素都不可见")
                    else:
                        logger.info(f"选择器 {selector} 未找到任何元素")
                except Exception as e:
                    logger.info(f"尝试选择器 {selector} 时出错: {e}")
                    continue
            
            if not valid_selector:
                logger.warning("未找到有效的文章选择器，使用默认选择器")
                valid_selector = ".search-list-box .list-item"
            
            target_count = float('inf') if article_count is None else article_count
            current_count = 0
            last_count = 0
            consecutive_no_change = 0
            max_no_change_attempts = 3
            load_more_clicks = 0
            max_load_more_clicks = 30
            
            while current_count < target_count:
                articles_elements = driver.find_elements(By.CSS_SELECTOR, valid_selector)
                current_count = len(articles_elements)
                logger.info(f"当前已加载 {current_count} 篇文章")
                if current_count >= target_count:
                    logger.info(f"已获取足够数量的文章: {current_count} >= {target_count}")
                    break
                
                if current_count == last_count:
                    consecutive_no_change += 1
                    logger.warning(f"滚动后未加载新文章，连续 {consecutive_no_change}/{max_no_change_attempts} 次")
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
            
            rendered_html = driver.page_source
            html_path = os.path.join(output_dir, "search_response_dynamic.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(rendered_html)
            logger.info(f"已保存动态渲染的 HTML 到 {html_path}")
            
            screenshot_path = os.path.join(output_dir, "search_screenshot.png")
            driver.save_screenshot(screenshot_path)
            logger.info(f"已保存页面截图到 {screenshot_path}")
            
            soup = BeautifulSoup(rendered_html, 'html.parser')
            articles = soup.select(valid_selector)
            total_articles = len(articles)
            logger.info(f"选择器 '{valid_selector}' 找到 {total_articles} 个结果")
            
            end_index = total_articles if article_count is None else min(start_index + article_count, total_articles)
            selected_articles = articles[start_index:end_index]
            logger.info(f"根据范围参数筛选，实际处理文章 {start_index} 至 {end_index-1}，共 {len(selected_articles)} 篇")
            
            # 实时保存（JSON Lines格式）并进行查重
            with open(realtime_file, "a", encoding="utf-8") as f:
                for i, article in enumerate(selected_articles):
                    article_index = start_index + i
                    article_text = article.text.strip()
                    article_hash = hashlib.md5(article_text.encode('utf-8')).hexdigest()
                    if article_hash in seen_hashes:
                        logger.info(f"发现重复文章: index {article_index}，已跳过")
                        continue
                    seen_hashes.add(article_hash)
                    article_info = {
                        "index": article_index,
                        "text": article_text[:100] + "..." if len(article_text) > 100 else article_text,
                        "html_structure": str(article)[:500] + "..." if len(str(article)) > 500 else str(article)
                    }
                    links = article.select("a")
                    if links:
                        article_info["links"] = []
                        for link in links:
                            href = link.get('href', '')
                            text_link = link.text.strip()
                            article_info["links"].append({
                                "href": href,
                                "text": text_link
                            })
                    f.write(json.dumps(article_info, ensure_ascii=False) + "\n")
                    total_saved += 1
                    logger.info(f"已实时保存文章 {article_index}，当前总保存数量: {total_saved}")
            
            logger.info(f"实时保存完成，总计保存 {total_saved} 篇去重后的文章")
            return True
            
        finally:
            driver.quit()
            
    except Exception as e:
        logger.error(f"动态请求调试过程中出错: {e}", exc_info=True)
        return False

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CSDN 搜索调试工具")
    parser.add_argument("--query", required=True, help="搜索查询词")
    parser.add_argument("--output-dir", default="debug_output", help="调试输出目录")
    parser.add_argument("--method", choices=["all", "static", "dynamic", "api"], default="dynamic", help="调试方法")
    parser.add_argument("--start", type=int, default=0, help="开始爬取的文章索引（从0开始）")
    parser.add_argument("--count", type=int, default=None, help="要爬取的文章数量，不指定则爬取所有可获取的文章")
    
    args = parser.parse_args()
    
    if args.method == "all":
        debug_csdn_search(args.query, args.output_dir, args.start, args.count)
    elif args.method == "static":
        logger.error("静态请求方法未实现。")
    elif args.method == "dynamic":
        debug_csdn_search_dynamic(args.query, args.output_dir, args.start, args.count)
    elif args.method == "api": 
        debug_csdn_search_api(args.query, args.output_dir, args.start, args.count)
    
    logger.info("调试完成")
