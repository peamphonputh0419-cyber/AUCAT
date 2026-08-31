import asyncio
import json
import os
import re
import sqlite3
import urllib.parse
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from apscheduler.schedulers.background import BackgroundScheduler
from playwright.sync_api import sync_playwright

DB_NAME = "audition_fashion.db"
progress_queue = asyncio.Queue()
is_scraping_running = False  # ป้องกันไม่ให้รันการสแกนซ้อนกัน


def get_db_connection():
    """เชื่อมต่อกับ SQLite Database"""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """สร้างตาราง items ใน SQLite หากยังไม่มี"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")  # เพิ่มประสิทธิภาพอ่าน-เขียน
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_detail TEXT,
            gender TEXT NOT NULL,
            image_url TEXT UNIQUE NOT NULL,
            source_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def run_hourly_scraping_job():
    """Background Job สำหรับสแกนข้อมูลอัตโนมัติทุก 1 ชั่วโมง"""
    global is_scraping_running
    if is_scraping_running:
        print(
            "⏳ [Background Job] สแกนรอบก่อนหน้านี้ยังไม่เสร็จ ข้ามรอบนี้ไป..."
        )
        return

    print("⏰ [Background Job] กำลังเริ่มดึงข้อมูลโปรโมชันและกิจกรรม...")
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    run_full_scraper_sync(loop)
    print("✅ [Background Job] ดึงข้อมูลเรียบร้อยแล้ว!")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. เริ่มต้นระบบและสร้าง Database Table
    init_db()

    # 2. ตั้งเวลาสแกนอัตโนมัติทุกๆ 1 ชั่วโมง
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_hourly_scraping_job, "interval", hours=1)
    scheduler.start()

    # 3. รันสแกนทันที 1 รอบเมื่อ Server เริ่มเปิด (Startup Check)
    asyncio.create_task(asyncio.to_thread(run_hourly_scraping_job))

    yield

    # Shutdown Scheduler เมื่อปิด Server
    scheduler.shutdown()


app = FastAPI(title="Audition Item Catalog API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def classify_source_type(text: str) -> str:
    """จำแนกหมวดหมู่ที่มาของไอเทม"""
    text = text.lower()

    if any(
        k in text
        for k in [
            "daily login",
            "daily_login",
            "daily",
            "login",
            "เช็คชื่อ",
            "รายวัน",
            "ล็อกอินรายวัน",
            "เข้าเกมประจำวัน",
            "เข้าเกมรับฟรี",
        ]
    ):
        return "Daily Login"
    elif any(
        k in text
        for k in [
            "refill",
            "re-fill",
            "รีฟิล",
            "เติมเงินสะสม",
            "first refill",
            "refill bonus",
        ]
    ):
        return "Refill"
    elif any(
        k in text
        for k in [
            "golden gacha",
            "golden_gacha",
            "โกลเด้นกาชา",
            "โกลเด้น กาชา",
            "ตู้ทอง",
            "golden point",
        ]
    ):
        return "Golden Gacha"
    elif any(
        k in text
        for k in [
            "gacha",
            "gachapon",
            "กาชา",
            "ตู้สุ่ม",
            "premium gacha",
            "exclusive gacha",
        ]
    ):
        return "Gacha"
    elif any(
        k in text for k in ["สอยดาว", "หมุนดาว", "สุ่มดาว", "star promotion"]
    ):
        return "สอยดาว"
    elif any(
        k in text
        for k in [
            "เติมเงิน",
            "topup",
            "top-up",
            "บัตรเติมเงิน",
            "สะสมยอดเติม",
            "โปรเติม",
        ]
    ):
        return "TopUp"
    elif any(
        k in text
        for k in [
            "bonus time",
            "โบนัสไทม์",
            "x2",
            "exp",
            "den",
            "คุ้มสองต่อ",
            "แถมพิเศษ",
        ]
    ):
        return "Bonus Time"
    elif any(
        k in text
        for k in [
            "web shop",
            "webshop",
            "ร้านค้าเว็บ",
            "ซื้อผ่านเว็บ",
            "แฟลชเซลล์",
            "itemshop",
            "item shop",
            "out mall",
        ]
    ):
        return "Web Shop"
    elif any(
        k in text for k in ["กิจกรรม", "event", "แจกฟรี", "สะสมรอบเต้น", "เต้นแลก"]
    ):
        return "กิจกรรมฟรี"

    return "โปรโมชันพิเศษ"


def parse_items_from_text(text: str):
    """
    ฟังก์ชันสกัดชื่อไอเทม เพศ และหมวดหมู่จากบรรทัดข้อความ
    รองรับทั้งแบบ Daily Login (• item ผู้ชาย – ...), Refill, และโปรโมชันปกติ
    """
    items_found = []
    clean_text = text.replace("\xa0", " ").strip()

    # ตรวจจับคำระบุเพศ
    gender_match = re.search(
        r"(?:item\s*)?(?:ไอเทม\s*)?(ผู้ชาย|ผู้หญิง|ชาย|หญิง)",
        clean_text,
        re.IGNORECASE,
    )
    if not gender_match:
        return items_found

    raw_gender = gender_match.group(1)
    gender = "ชาย" if "ชาย" in raw_gender else "หญิง"

    # ตัดสัญลักษณ์หน้าและคำระบุเพศออก เหลือเฉพาะชื่อไอเทม
    content = re.sub(r"^[•\-\s]*", "", clean_text)
    content = re.sub(
        r"^(?:item\s*)?(?:ไอเทม\s*)?(?:ผู้ชาย|ผู้หญิง|ชาย|หญิง)\s*[\:–\-]?\s*",
        "",
        content,
        flags=re.IGNORECASE,
    ).strip()

    if not content:
        return items_found

    # แยกไอเทมหลายชิ้นในบรรทัดเดียวกันด้วยเครื่องหมาย /
    sub_items = [item.strip() for item in content.split("/") if item.strip()]

    for raw_name in sub_items:
        category = "ชุดแฟชั่น"
        if any(k in raw_name for k in ["ทรงผม", "Hair", "ผม"]):
            category = "ทรงผม"
        elif any(k in raw_name for k in ["หน้า", "Face"]):
            category = "ใบหน้า"
        elif any(k in raw_name for k in ["เสื้อ", "Top", "Shirt", "T-shirt"]):
            category = "เสื้อ"
        elif any(
            k in raw_name
            for k in ["กางเกง", "กระโปรง", "Pants", "Skirt", "Bottom"]
        ):
            category = "กางเกง/กระโปรง"
        elif any(k in raw_name for k in ["รองเท้า", "Shoes", "Boots"]):
            category = "รองเท้า"
        elif any(
            k in raw_name for k in ["ปีก", "เครื่องประดับ", "Wing", "Accessory"]
        ):
            category = "เครื่องประดับ"
        elif any(
            k in raw_name
            for k in ["เซต", "Uniform", "Set", "Couple", "ชุดแต่งกาย"]
        ):
            category = "เซตเสื้อผ้า"

        items_found.append(
            {"name": raw_name, "gender": gender, "category": category}
        )

    return items_found


def scrape_article_detail_sync(page, article_url: str, article_title: str):
    """สแกนรายละเอียดภายในบทความและบันทึกลง Database"""
    conn = get_db_connection()
    cursor = conn.cursor()

    source_type = classify_source_type(f"{article_title} {article_url}")
    junk_keywords = [
        "download",
        "register",
        "client",
        "button",
        "banner",
        "logo",
        "icon",
        "header",
        "footer",
        "sidebar",
        "qr",
        "truemoney",
        "promptpay",
        "facebook",
        "line",
    ]

    saved_count = 0
    try:
        page.goto(article_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(1000)

        # สแกนหาข้อความและรูปภาพในโครงสร้าง Daily Login / Refill / โปรปกติ
        elements = page.query_selector_all(
            ".entry-content p, .entry-content td, .entry-content li, article p, article td, article li"
        )

        for el in elements:
            text = el.inner_text().strip()

            if any(k in text for k in ["ชาย", "หญิง", "ผู้ชาย", "ผู้หญิง"]):
                parsed_items = parse_items_from_text(text)

                if not parsed_items:
                    continue

                # ค้นหารูปภาพใกล้เคียงไอเทมชิ้นนั้น
                img_url = ""
                try:
                    img_el = el.query_selector("img")
                    if not img_el:
                        img_el = el.evaluate_handle(
                            "el => el.previousElementSibling?.querySelector('img') || el.nextElementSibling?.querySelector('img') || el.closest('tr')?.querySelector('img')"
                        )

                    if img_el:
                        src = img_el.get_attribute("src") or img_el.get_attribute(
                            "data-src"
                        )
                        if src and not any(j in src.lower() for j in junk_keywords):
                            img_url = urllib.parse.urljoin(article_url, src)
                except:
                    pass

                if not img_url:
                    continue

                for item in parsed_items:
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO items (name, category, source_type, source_detail, gender, image_url, source_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            item["name"],
                            item["category"],
                            source_type,
                            article_title,
                            item["gender"],
                            img_url,
                            article_url,
                        ),
                    )
                    saved_count += 1

        conn.commit()
    except Exception as e:
        print(f"⚠️ Error {article_url}: {e}")
    finally:
        conn.close()

    return saved_count


def run_full_scraper_sync(loop=None):
    global is_scraping_running
    is_scraping_running = True
    total_saved = 0

    categories = [
        (
            "Promotion",
            "https://audition.playpark.com/th-th/category/news/promotion/",
        ),
        (
            "Event",
            "https://audition.playpark.com/th-th/category/news/event/",
        ),
    ]

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox"],
            )
            page = browser.new_page()

            for cat_name, base_url in categories:
                page_num = 1

                while True:
                    target_url = (
                        base_url
                        if page_num == 1
                        else f"{base_url}page/{page_num}/"
                    )

                    if loop:
                        asyncio.run_coroutine_threadsafe(
                            progress_queue.put({
                                "status": "progress",
                                "category": cat_name,
                                "page": page_num,
                                "message": f"กำลังสแกนหมวด {cat_name} หน้าที่ {page_num}...",
                                "total_items": total_saved,
                                "total": total_saved,
                            }),
                            loop,
                        )

                    try:
                        response = page.goto(
                            target_url,
                            wait_until="domcontentloaded",
                            timeout=30000,
                        )
                        if response and response.status == 404:
                            break

                        articles = page.query_selector_all(
                            "article a, .post a, .entry-title a"
                        )
                        if not articles:
                            break

                        news_links = []
                        for a in articles:
                            href = a.get_attribute("href")
                            title = a.inner_text().strip()
                            if (
                                href
                                and len(title) > 5
                                and href not in [x[0] for x in news_links]
                            ):
                                news_links.append((href, title))

                        if not news_links:
                            break

                        for href, title in news_links:
                            saved = scrape_article_detail_sync(
                                page, href, title
                            )
                            total_saved += saved
                            if loop:
                                asyncio.run_coroutine_threadsafe(
                                    progress_queue.put({
                                        "status": "progress",
                                        "category": cat_name,
                                        "page": page_num,
                                        "message": f"ดึงข้อมูล: {title[:25]}... (+{saved})",
                                        "total_items": total_saved,
                                        "total": total_saved,
                                    }),
                                    loop,
                                )

                        page_num += 1
                    except Exception as e:
                        print(f"⚠️ Error: {e}")
                        break

            browser.close()
    finally:
        is_scraping_running = False

    if loop:
        asyncio.run_coroutine_threadsafe(
            progress_queue.put({
                "status": "completed",
                "message": "สแกนข้อมูลโปรโมชันและกิจกรรมเรียบร้อยแล้ว!",
                "total_items": total_saved,
                "total": total_saved,
            }),
            loop,
        )


@app.get("/")
def read_index():
    return FileResponse("index.html")


@app.get("/api/items")
def get_items(
    search: str = Query(""),
    category: str = Query("ทั้งหมด"),
    source: str = Query("ทั้งหมด"),
    gender: str = Query("ทั้งหมด"),
    sort: str = Query("desc"),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    conn = get_db_connection()
    cursor = conn.cursor()

    count_query = "SELECT COUNT(*) FROM items WHERE 1=1"
    query = "SELECT id, name, category, source_type, source_detail, gender, image_url, source_url FROM items WHERE 1=1"
    params = []

    if search:
        clause = " AND (name LIKE ? OR source_detail LIKE ?)"
        query += clause
        count_query += clause
        params.extend([f"%{search}%", f"%{search}%"])

    if category != "ทั้งหมด":
        clause = " AND category = ?"
        query += clause
        count_query += clause
        params.append(category)

    if source != "ทั้งหมด":
        clause = " AND source_type = ?"
        query += clause
        count_query += clause
        params.append(source)

    if gender != "ทั้งหมด":
        clause = (
            " AND (gender LIKE ? OR gender = 'ทั้งหมด' OR gender = 'ชาย/หญิง')"
        )
        query += clause
        count_query += clause
        params.append(f"%{gender}%")

    cursor.execute(count_query, params)
    total_items = cursor.fetchone()[0]

    order_direction = "DESC" if sort.lower() == "desc" else "ASC"
    offset = (page - 1) * limit
    query += f" ORDER BY id {order_direction} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = cursor.fetchall()

    conn.close()

    response = JSONResponse(
        content={
            "items": [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "category": r["category"],
                    "sourceType": r["source_type"],
                    "detail": r["source_detail"],
                    "gender": r["gender"],
                    "img": r["image_url"],
                    "url": r["source_url"],
                }
                for r in rows
            ],
            "total": total_items,
            "page": page,
            "totalPages": (total_items + limit - 1) // limit,
        }
    )

    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


@app.get("/api/scrape-stream")
async def scrape_stream():
    global is_scraping_running
    if is_scraping_running:
        return StreamingResponse(
            iter([
                f"data: {json.dumps({'status': 'progress', 'message': 'ระบบกำลังทำการสแกนอยู่แล้ว...', 'total_items': 0, 'total': 0}, ensure_ascii=False)}\n\n"
            ]),
            media_type="text/event-stream",
        )

    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, run_full_scraper_sync, loop)

    async def event_generator():
        while True:
            data = await progress_queue.get()
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            if data.get("status") == "completed":
                break

    return StreamingResponse(event_generator(), media_type="text/event-stream")
