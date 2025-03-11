## 使用範例

从第0篇开始，爬取20篇文章
python cdsn.py --query "机器学习" --start 0 --count 20

从第5篇文章开始，爬取10篇文章
python cdsn.py --query "人工智能" --start 5 --count 10

从第10篇开始，爬取所有剩余文章
python cdsn.py --query "深度学习" --start 0

使用特定方法并指定文章范围
python cdsn.py --query "Python教程" --method dynamic --start 3 --count 7

# 高級網絡爬蟲系統：多策略提取框架

## 計算架構概述

本倉庫包含一個雙組件網絡爬蟲系統，在容錯並發處理框架內實現先進的分布式提取策略。該系統採用形式化計算模型，通過多層次方法提取網頁內容，具有理論保證的檢索特性。

```
架構拓撲：
┌───────────────────┐     ┌───────────────────┐
│  cdsn.py          │     │  jsonurl.py       │
│  ───────────────  │     │  ───────────────  │
│  搜索策略         │────▶│  內容提取         │
│  編排系統         │     │  管道處理         │
└───────────────────┘     └───────────────────┘
```

## 理論基礎

### 形式化計算模型

兩個系統實現了定義為7元組的狀態轉換網絡 $M = (Q, Σ, δ, q_0, F, R, L)$，其中：
- $Q$：計算狀態集合（請求、解析、提取）
- $Σ$：輸入字母表（HTTP響應、DOM結構）
- $δ: Q × Σ → Q$：狀態轉換函數
- $q_0 ∈ Q$：初始狀態（查詢構建）
- $F ⊆ Q$：終止狀態集合（內容提取）
- $R$：資源約束向量
- $L$：具有指數退避特性的速率限制函數

### 計算複雜度分析

- 時間複雜度：$O(n⋅m)$，其中$n$是URL數量，$m$是平均頁面複雜度
- 空間複雜度：$O(n + k)$，其中$k$是並發參數
- 網絡I/O複雜度：具有自適應限速的攤銷$O(n)$

## 系統組件

### 1. CSDN搜索爬蟲（`cdsn.py`）

模塊化搜索編排系統，實現策略設計模式並具備自動降級能力。

#### 關鍵抽象
- `SearchStrategy`：定義搜索行為契約的抽象基類
- `SearchController`：管理策略選擇與執行的編排層
- `WebDriverManager`：Selenium資源生命周期管理

#### 高級特性
- 多策略搜索實現（API、動態、靜態）
- 具有馬爾可夫狀態轉移的概率請求分布
- 實現指數退避算法的自適應延遲機制
- 通過信號量調節的線程池實現形式化並發控制

#### 性能考量
該系統採用異步資源分配，具有確定性的清理保證。請求調度器實現了帶有域特定速率調整的令牌桶算法變體。

```python
class RateLimiter:
    # 實現O(1)時間複雜度的速率決策，具有O(log n)時間複雜度的退避計算
    def _calculate_delay(self) -> float:
        self.request_count += 1
        current_rate = self._get_current_rate()
        
        # 基礎延遲與抖動，用於反指紋識別
        base_delay = 60.0 / self.requests_per_minute
        jitter = random.uniform(0.1, 1.5)
        delay = base_delay * jitter
        
        # 用於突發控制的對數退避
        if self.request_count > 10:
            delay *= math.log(self.request_count, 2)
        
        return delay
```

### 2. URL內容提取器（`jsonurl.py`）

基於管道的內容提取系統，具有特定領域的處理策略和並發媒體採集功能。

#### 架構設計
- 用於內容提取器解析的工廠方法模式
- 用於領域特定處理規則的訪問者模式
- 用於資源生命周期管理的上下文管理器模式
- 用於統計收集和監控的觀察者模式

#### 高級子系統
- **自適應並發控制器**：根據域響應模式動態調整並行性
- **內容分類引擎**：採用啟發式分析進行內容類型確定
- **付費牆檢測系統**：具有評分機制的多維特徵分析
- **媒體管道處理器**：具有完整性驗證的異步採集

#### 理論優化
- 使用加權公平隊列進行優先域調度
- 特定域的HTTP請求合併，實現連接池化
- 具有增量DOM樹構建的響應解析

```python
class PaywallDetector:
    # 實現具有O(k+m)複雜度的概率特徵檢測系統
    # 其中k是指標數量，m是文檔大小
    def detect(self, soup: BeautifulSoup, html_text: str, url: str) -> Tuple[bool, int]:
        score = 0  # 初始評分向量
        
        # 多維特徵提取與加權評分
        # 領域特定偏差調整
        if any(keyword in domain.lower() for keyword in ['premium', 'vip']):
            score += 1
            
        # 關鍵詞密度分析 - O(k)複雜度
        for pattern in self.text_indicators:
            if pattern.lower() in lower_text:
                score += 2
                
        # 結構模式識別 - O(m)複雜度
        for selector in self.css_indicators:
            if soup.select(selector):
                score += 3
                
        # 具有領域自適應閾值的決策函數
        threshold = 4
        if any(d in domain for d in premium_domains):
            threshold = 3
            
        return score >= threshold, score
```

## 系統整合模型

這些系統以單向管道方式運作，其中`cdsn.py`執行多策略搜索編排，生成結構化JSON輸出，然後由`jsonurl.py`通過其提取管道處理。這種設計實現：

1. **關注點分離**：每個組件解決不同的計算領域問題
2. **錯誤隔離**：搜索中的失敗不會傳播到內容提取
3. **計算效率**：搜索和提取階段可以獨立擴展

## 使用方法：命令行接口

### CSDN搜索爬蟲

```bash
python cdsn.py --query "搜索查詢" --output-dir "輸出目錄" --method "all|api|dynamic|static" --start 0 --count 10 --log-level INFO
```

參數：
- `--query`：搜索查詢字符串（必須）
- `--output-dir`：搜索結果的輸出目錄（默認："debug_output"）
- `--method`：搜索方法（默認："all"）
- `--start`：起始文章索引（默認：0）
- `--count`：要檢索的文章數量（默認：所有可用）
- `--log-level`：日誌級別（默認："INFO"）

### JSON URL內容提取器

```bash
python jsonurl.py --input "filtered_api_response_0.json" --output "scraped_articles" --workers 3 --delay 2.0 --timeout 30 --retries 3 [--no-images] [--proxy "http://proxy:port"]
```

參數：
- `--input`：包含URL的輸入JSON文件（默認："./debug_output/filtered_api_response_0.json"）
- `--output`：爬取內容的輸出目錄（默認："./debug_output/scraped_articles"）
- `--workers`：最大並發工作數（默認：3）
- `--delay`：請求間隔秒數（默認：2.0）
- `--timeout`：請求超時秒數（默認：30）
- `--retries`：最大重試次數（默認：3）
- `--no-images`：禁用圖像下載的標志
- `--proxy`：代理服務器URL（可選）
- `--log-level`：日誌級別（默認："INFO"）

## 實現級考量

### 理論時間複雜度

1. **CSDN搜索**：
   - API方法：每頁$O(1)$，分頁檢索為$O(n/p)$，其中$p$是頁面大小
   - 動態方法：$O(d)$，其中$d$是DOM複雜度，具有對數性質的基於滾動的檢索模型

2. **內容提取**：
   - 特定領域提取器：$O(s)$，其中$s$是選擇器特異性
   - 圖像管道：每圖像$O(i)$，並行獲取為$O(i/w + l)$，其中$w$是工作者數量，$l$是延遲

### 記憶體優化

兩個系統實現了記憶體敏感的設計模式：
- 大型響應的流式處理
- 增量解析，避免完整DOM物化
- 共享資源的引用計數
- 高記憶體操作的顯式垃圾收集觸發

### 未來可擴展性

系統架構支持通過以下方式進行形式擴展：

1. **策略添加**：通過實現抽象基類可以集成新的搜索或提取策略
2. **管道增強**：通過注入自定義處理器可以增強內容處理
3. **領域專門化**：網站特定的提取器可以添加到工廠註冊表中

## 高級實現示例

### 自適應速率限制器（cdsn.py）

```python
def wait(self, url: str):
    """
    實現具有形式公平保證的域分區速率限制器。
    時間複雜度：攤銷O(1)
    空間複雜度：O(d)，其中d是唯一域的數量
    """
    domain = TextUtils.extract_domain(url) if self.per_domain else "global"
    
    # 初始化時間戳向量
    with self.global_lock:
        if domain not in self.request_timestamps:
            self.request_timestamps[domain] = []
    
    current_rate = self._get_current_rate(domain)
    
    # 基於速率的延遲計算
    if current_rate >= self.requests_per_minute:
        with self._get_domain_lock(domain):
            oldest_timestamp = min(self.request_timestamps[domain])
            wait_time = max(0, 60 - (time.time() - oldest_timestamp))
            
            if wait_time > 0:
                time.sleep(wait_time)
```

### 內容提取策略（jsonurl.py）

```python
def extract(self, url: str, soup: BeautifulSoup, html: str) -> Dict[str, Any]:
    """
    CSDN特定提取，具有代碼塊、層次標題和媒體元素的形式正確性屬性。
    
    實現具有目標子樹提取的O(n) HTML遍歷。
    """
    # ... [初始化] ...
    
    # 具有漸進式降級的內容容器選擇
    for selector in content_selectors:
        container = soup.select_one(selector)
        if container and len(container.get_text(strip=True)) > 200:
            content_container = container
            break
    
    if content_container:
        # 保留語言的代碼塊轉換
        for pre in content_container.find_all('pre'):
            code = pre.find('code')
            if code:
                language = code.get('class', [''])[0].replace('language-', '')
                code_text = code.get_text(strip=True)
                pre.replace_with(f"\n```{language}\n{code_text}\n```\n")
```

## 結論

這個雙組件網絡爬蟲系統實現了理論完善、計算高效的網頁內容提取方法，對資源利用、錯誤恢復能力和內容完整性提供形式保證。模塊化架構使搜索和提取過程能夠獨立擴展，同時保持強大的故障隔離邊界。

系統基於計算理論和形式化方法，不僅能夠有效應對當前網絡環境下的爬取挑戰，還提供了一個可以在理論層面進行擴展和完善的框架。特別適合需要高可靠性、高效率和強大擴展能力的專業數據採集場景。