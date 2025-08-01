import asyncio
import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import xml.etree.ElementTree as ET

import aiohttp
import feedparser
from crawl4ai import AsyncWebCrawler
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

class Config:
    def __init__(self):
        # Validate required environment variables
        required_vars = [
            'WORLD_NEWS_WEBHOOK_URL',
            'CANADA_NEWS_WEBHOOK_URL', 
            'GEMINI_API_KEY',
            'GLOBAL_NEWS_URL',
            'CBC_NEWS_URL',
            'CBC_BC_NEWS_URL',
            'REUTERS_URL'
        ]
        
        for var_name in required_vars:
            if not os.getenv(var_name):
                print(f"❌ Missing required environment variable: {var_name}")
                print("Please check your .env file")
                sys.exit(1)
        
        # RSS Feed Configuration with Channel Mapping
        self.RSS_FEEDS = [
            {
                'name': 'Global News Canada',
                'url': os.getenv('GLOBAL_NEWS_URL'),
                'channel': 'canada-news',
                'webhook_url': os.getenv('CANADA_NEWS_WEBHOOK_URL')
            },
            {
                'name': 'CBC Canada',
                'url': os.getenv('CBC_NEWS_URL'),
                'channel': 'canada-news',
                'webhook_url': os.getenv('CANADA_NEWS_WEBHOOK_URL')
            },
            {
                'name': 'CBC British Columbia',
                'url': os.getenv('CBC_BC_NEWS_URL'),
                'channel': 'canada-news',
                'webhook_url': os.getenv('CANADA_NEWS_WEBHOOK_URL')
            },
            {
                'name': 'Reuters',
                'url': os.getenv('REUTERS_URL'),
                'channel': 'world-news',
                'webhook_url': os.getenv('WORLD_NEWS_WEBHOOK_URL')
            }
        ]
        
        # AI Configuration
        self.GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
        self.AI_MODEL = 'gemini-2.5-flash'
        
        # File paths
        self.DATA_DIR = Path(__file__).parent / 'data'
        self.TITLES_FILE = self.DATA_DIR / 'processed_titles.json'
        
        # Bot settings
        self.CHECK_INTERVAL = 300  # 5 minutes in seconds
        self.MAX_STORED_TITLES = 1000
        self.REQUEST_TIMEOUT = 30
        
        # Date filtering - Canada timezone
        self.TIMEZONE = 'America/Toronto'
        
        # Logging
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'info')
        
        self._print_config()
    
    def _print_config(self):
        print('📝 Configuration loaded:')
        print(f'   RSS Feeds: {len(self.RSS_FEEDS)} configured')
        for feed in self.RSS_FEEDS:
            print(f'     - {feed["name"]} → {feed["channel"]}')
        print(f'   Timezone: {self.TIMEZONE}')
        print(f'   AI Model: {self.AI_MODEL}')
        print(f'   Check Interval: {self.CHECK_INTERVAL}s')

class NewsBot:
    def __init__(self):
        self.config = Config()
        self.processed_titles: Set[str] = set()
        self.is_running = False
        self.crawler = None
        self.session = None
        self.stats = {
            'total_processed': 0,
            'last_check': None,
            'errors': 0,
            'items_today': 0,
            'items_filtered': 0
        }
    
    async def start(self):
        print('🤖 Starting Multi-Feed News Bot...')
        await self.load_processed_titles()
        
        # Initialize HTTP session and crawler
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.REQUEST_TIMEOUT)
        )
        self.crawler = AsyncWebCrawler(verbose=True)
        await self.crawler.__aenter__()
        
        self.is_running = True
        await self.schedule_checks()
    
    async def load_processed_titles(self):
        try:
            # Ensure data directory exists
            self.config.DATA_DIR.mkdir(exist_ok=True)
            
            if self.config.TITLES_FILE.exists():
                with open(self.config.TITLES_FILE, 'r', encoding='utf-8') as f:
                    titles = json.load(f)
                self.processed_titles = set(titles)
                print(f'📚 Loaded {len(titles)} processed titles')
            else:
                print('📝 No previous titles found, starting fresh')
                self.processed_titles = set()
        except Exception as error:
            print(f'❌ Error loading titles: {error}')
            self.processed_titles = set()
    
    async def save_processed_titles(self):
        try:
            titles = list(self.processed_titles)
            recent_titles = titles[-self.config.MAX_STORED_TITLES:]
            
            with open(self.config.TITLES_FILE, 'w', encoding='utf-8') as f:
                json.dump(recent_titles, f, ensure_ascii=False, indent=2)
            
            print(f'💾 Saved {len(recent_titles)} titles')
        except Exception as error:
            print(f'❌ Error saving titles: {error}')
            self.stats['errors'] += 1
    
    def is_published_today(self, pub_date: str) -> bool:
        if not pub_date:
            return False
        
        try:
            # Handle different timezone formats
            date_str = pub_date.strip()
            
            # Replace common timezone abbreviations with offsets
            timezone_replacements = {
                ' EDT': ' -0400',  # Eastern Daylight Time
                ' EST': ' -0500',  # Eastern Standard Time
                ' CDT': ' -0500',  # Central Daylight Time
                ' CST': ' -0600',  # Central Standard Time
                ' MDT': ' -0600',  # Mountain Daylight Time
                ' MST': ' -0700',  # Mountain Standard Time
                ' PDT': ' -0700',  # Pacific Daylight Time
                ' PST': ' -0800',  # Pacific Standard Time
                ' GMT': ' +0000',  # Greenwich Mean Time
                ' UTC': ' +0000'   # Coordinated Universal Time
            }
            
            for abbr, offset in timezone_replacements.items():
                if date_str.endswith(abbr):
                    date_str = date_str.replace(abbr, offset)
                    break
            
            # Try parsing with timezone offset
            try:
                article_date = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
            except ValueError:
                # Fallback: try without timezone and assume Eastern Time
                try:
                    article_date = datetime.strptime(pub_date.rsplit(' ', 1)[0], '%a, %d %b %Y %H:%M:%S')
                    # Assume Eastern timezone if no timezone info
                    eastern_tz = pytz.timezone('America/New_York')
                    article_date = eastern_tz.localize(article_date)
                except ValueError:
                    # Last resort: try parsing with feedparser
                    import time
                    from email.utils import parsedate_tz, mktime_tz
                    
                    parsed = parsedate_tz(pub_date)
                    if parsed:
                        timestamp = mktime_tz(parsed)
                        article_date = datetime.fromtimestamp(timestamp, tz=pytz.UTC)
                    else:
                        raise ValueError(f"Unable to parse date: {pub_date}")
            
            now = datetime.now()
            
            # Convert to Canada timezone
            canada_tz = pytz.timezone(self.config.TIMEZONE)
            canada_time = now.astimezone(canada_tz)
            article_canada_time = article_date.astimezone(canada_tz)
            
            # Check if same date
            return canada_time.date() == article_canada_time.date()
            
        except Exception as error:
            print(f'⚠️  Invalid date format: {pub_date} - Error: {error}')
            # If we can't parse the date, assume it's recent (better to include than exclude)
            return True
    
    async def fetch_rss_feed(self, feed_config: Dict) -> List[Dict]:
        try:
            print(f'📡 Fetching {feed_config["name"]}: {feed_config["url"]}')
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/rss+xml, application/xml, text/xml, application/atom+xml, */*',
            }
            
            async with self.session.get(feed_config['url'], headers=headers) as response:
                if response.status != 200:
                    raise Exception(f'HTTP {response.status}')
                
                content = await response.text()
            
            # Parse RSS feed
            feed = feedparser.parse(content)
            items = feed.entries
            
            print(f'📰 Found {len(items)} RSS items from {feed_config["name"]}')
            
            processed_items = []
            for item in items:
                processed_items.append({
                    'title': getattr(item, 'title', ''),
                    'link': getattr(item, 'link', ''),
                    'pub_date': getattr(item, 'published', ''),
                    'description': getattr(item, 'description', ''),
                    'feed_name': feed_config['name'],
                    'channel': feed_config['channel'],
                    'webhook_url': feed_config['webhook_url']
                })
            
            # Filter for today's articles only
            today_items = []
            for item in processed_items:
                is_today = self.is_published_today(item['pub_date'])
                if not is_today and item['pub_date']:
                    print(f'📅 Skipping old article: {item["title"][:50]}... ({item["pub_date"]})')
                    self.stats['items_filtered'] += 1
                elif is_today:
                    self.stats['items_today'] += 1
                    today_items.append(item)
            
            print(f'✅ {len(today_items)} articles from today found in {feed_config["name"]}')
            return today_items
            
        except Exception as error:
            print(f'❌ Error fetching RSS from {feed_config["name"]}: {error}')
            self.stats['errors'] += 1
            return []
    
    async def scrape_with_crawl4ai(self, url: str) -> Optional[str]:
        try:
            print(f'🕷️ Scraping: {url}')
            
            result = await self.crawler.arun(
                url=url,
                word_count_threshold=10,
                bypass_cache=True
            )
            
            if result.success and result.markdown:
                print('✅ Scraping successful')
                return result.markdown
            else:
                raise Exception('Scraping failed - no content returned')
                
        except Exception as error:
            print(f'❌ Crawl4AI error for {url}: {error}')
            return None
    
    async def process_with_ai(self, content: str, title: str) -> str:
        try:
            print(f'🤖 Processing with Gemini AI: {title[:50]}...')
            
            prompt = f"""你是一個專業的中文新聞編輯。請用繁體中文總結新聞內容，包含重點摘要和關鍵信息。

請總結以下新聞內容：

標題：{title}

內容：{content}

請提供：
1. 標題
2. 重點摘要
3. 關鍵信息"""
            
            payload = {
                "contents": [{
                    "parts": [{
                        "text": prompt
                    }]
                }],
                "generationConfig": {
                    "temperature": 0.3,
                    "topK": 40,
                    "topP": 0.95,
                    "maxOutputTokens": 10000,
                },
                "safetySettings": [
                    {
                        "category": "HARM_CATEGORY_HARASSMENT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_HATE_SPEECH",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    },
                    {
                        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
                    }
                ]
            }
            
            url = f'https://generativelanguage.googleapis.com/v1beta/models/{self.config.AI_MODEL}:generateContent'
            headers = {
                'Content-Type': 'application/json',
                'x-goog-api-key': self.config.GEMINI_API_KEY
            }
            
            async with self.session.post(url, json=payload, headers=headers) as response:
                if response.status != 200:
                    raise Exception(f'HTTP {response.status}')
                
                data = await response.json()
                
                if data.get('candidates') and len(data['candidates']) > 0:
                    generated_text = data['candidates'][0]['content']['parts'][0]['text']
                    print('✅ Gemini AI processing successful')
                    return generated_text
                else:
                    raise Exception('No response from Gemini API')
                    
        except Exception as error:
            print(f'❌ Gemini AI processing error: {error}')
            return '無法處理此新聞內容 - Gemini AI 處理失敗'
    
    async def send_to_discord(self, news_item: Dict, ai_summary: str) -> bool:
        try:
            channel_emoji = '🌍' if news_item['channel'] == 'world-news' else '🍁'
            color = 3447003 if news_item['channel'] == 'world-news' else 16711680  # Blue for world, Red for Canada
            
            embed = {
                "embeds": [{
                    "title": f"{channel_emoji} {news_item['title']}",
                    "description": ai_summary[:4000] + '...\n\n[內容過長已截斷]' if len(ai_summary) > 4000 else ai_summary,
                    "color": color,
                    "fields": [
                        {
                            "name": "📅 發布日期",
                            "value": news_item['pub_date'] or '未知',
                            "inline": True
                        },
                        {
                            "name": "📰 新聞來源",
                            "value": news_item['feed_name'],
                            "inline": True
                        },
                        {
                            "name": "🔗 原文連結",
                            "value": f"[點擊查看完整內容]({news_item['link']})",
                            "inline": False
                        }
                    ],
                    "footer": {
                        "text": f"頻道: {news_item['channel']}"
                    },
                    "timestamp": datetime.now().isoformat()
                }]
            }
            
            async with self.session.post(news_item['webhook_url'], json=embed) as response:
                if response.status not in [200, 204]:
                    raise Exception(f'HTTP {response.status}')
            
            print(f'✅ Sent to {news_item["channel"]}: {news_item["title"][:30]}...')
            return True
            
        except Exception as error:
            print(f'❌ Discord error for {news_item["channel"]}: {error}')
            self.stats['errors'] += 1
            return False
    
    async def process_news_item(self, item: Dict) -> bool:
        try:
            # Skip if already processed
            if item['title'] in self.processed_titles:
                return False
            
            print(f'\n🔄 Processing: {item["title"]}')
            print(f'📍 Channel: {item["channel"]} | Source: {item["feed_name"]}')
            
            # Scrape content
            content = await self.scrape_with_crawl4ai(item['link'])
            if not content:
                print('⏭️  Skipping due to scraping failure')
                return False
            
            # Process with AI
            ai_summary = await self.process_with_ai(content, item['title'])
            
            # Send to appropriate Discord channel
            sent = await self.send_to_discord(item, ai_summary)
            
            if sent:
                # Mark as processed
                self.processed_titles.add(item['title'])
                self.stats['total_processed'] += 1
                print('✅ Item processed successfully')
                return True
            
            return False
            
        except Exception as error:
            print(f'❌ Error processing item: {error}')
            self.stats['errors'] += 1
            return False
    
    async def check_for_news(self):
        if not self.is_running:
            return
        
        print('\n🔍 Checking for new news across all feeds...')
        self.stats['last_check'] = datetime.now()
        self.stats['items_today'] = 0
        self.stats['items_filtered'] = 0
        
        try:
            all_new_items = []
            
            # Fetch from all RSS feeds
            for feed_config in self.config.RSS_FEEDS:
                rss_items = await self.fetch_rss_feed(feed_config)
                new_items = [item for item in rss_items if item['title'] not in self.processed_titles]
                all_new_items.extend(new_items)
                
                print(f'📊 {feed_config["name"]}: {len(new_items)} new items from today')
                
                # Add delay between feeds to be respectful
                if feed_config != self.config.RSS_FEEDS[-1]:
                    await asyncio.sleep(1)
            
            print(f'\n📈 Summary: {len(all_new_items)} total new items to process')
            print(f'📅 Articles from today: {self.stats["items_today"]}')
            print(f'🗂️  Articles filtered (old): {self.stats["items_filtered"]}')
            
            if len(all_new_items) == 0:
                print('😴 No new items to process')
                return
            
            # Process items with delay to avoid rate limits
            processed = 0
            for i, item in enumerate(all_new_items):
                if not self.is_running:
                    break
                
                success = await self.process_news_item(item)
                if success:
                    processed += 1
                
                # Add delay between items
                if i < len(all_new_items) - 1:
                    print('⏳ Waiting 3 seconds...')
                    await asyncio.sleep(3)
            
            print(f'\n🎉 Processing complete! Successfully processed: {processed}/{len(all_new_items)}')
            
            # Save processed titles
            await self.save_processed_titles()
            
        except Exception as error:
            print(f'❌ Error in news check: {error}')
            self.stats['errors'] += 1
    
    async def schedule_checks(self):
        print(f'⏰ Scheduling checks every {self.config.CHECK_INTERVAL} seconds')
        print(f'🕐 Canada time zone: {self.config.TIMEZONE}')
        
        # Run immediately
        await self.check_for_news()
        
        # Then schedule regular checks
        while self.is_running:
            await asyncio.sleep(self.config.CHECK_INTERVAL)
            if self.is_running:
                await self.check_for_news()
    
    async def stop(self):
        print('🛑 Stopping Multi-Feed News Bot...')
        self.is_running = False
        
        if self.crawler:
            await self.crawler.__aexit__(None, None, None)
        
        if self.session:
            await self.session.close()
    
    def get_stats(self) -> Dict:
        return {
            **self.stats,
            'processed_titles_count': len(self.processed_titles),
            'is_running': self.is_running,
            'feeds': len(self.config.RSS_FEEDS)
        }

async def main():
    bot = NewsBot()
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        print(f'\n👋 Received signal {signum}, shutting down gracefully...')
        asyncio.create_task(shutdown(bot))
    
    async def shutdown(bot):
        await bot.stop()
        await bot.save_processed_titles()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start the bot
    try:
        await bot.start()
    except Exception as error:
        print(f'❌ Failed to start bot: {error}')
        await bot.stop()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())