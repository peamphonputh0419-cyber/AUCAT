import asyncio
import json
import sqlite3
import urllib.parse
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from playwright.sync_api import sync_playwright

app = FastAPI(title="Audition Item Catalog API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_NAME = "audition_fashion.db"
progress_queue = asyncio.Queue()


def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_detail TEXT,
            gender TEXT NOT NULL,
            image_url TEXT NOT NULL UNIQUE,
            source_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


@app.on_event("startup")
def startup_event():
    init_db()


def classify_source_type(text: str) -> str:
    text = text.lower()

    if any(
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
            "refill",
            "first refill",
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
        k in text
        for k in [
            "กิจกรรม",
            "event",
            "แจกฟรี",
            "ล็อกอิน",
            "login",
            "สะสมรอบเต้น",
            "เต้นแลก",
        ]
    ):
        return "กิจกรรมฟรี"

    return "โปรโมชันพิเศษ"


def classify_category(text: str) -> str:
    text = text.lower()
    if any(k in text for k in ["ผม", "hair", "ทรงผม", "hairstyle"]):
        return "ทรงผม"
    elif any(k in text for k in ["หน้า", "face", "ใบหน้า", "ตาสี"]):
        return "ใบหน้า"
    elif any(
        k in text
        for k in ["เสื้อ", "top", "แจ็คเก็ต", "สูท", "เชิ้ต", "t-shirt", "shirt"]
    ):
        return "เสื้อ"
    elif any(
        k in text
        for k in ["กางเกง", "กระโปรง", "pants", "skirt", "bottom", "ขา"]
    ):
        return "กางเกง/กระโปรง"
    elif any(k in text for k in ["รองเท้า", "shoes", "boots", "ส้นสูง"]):
        return "รองเท้า"
    elif any(
        k in text
        for k in [
            "ปีก",
            "wing",
            "กะโหลก",
            "แท่น",
            "เอฟเฟกต์",
            "เพ็ท",
            "สัตว์เลี้ยง",
            "ถือ",
            "กระเป๋า",
            "คทา",
            "บัฟ",
        ]
    ):
        return "เครื่องประดับ"
    return "ชุดแฟชั่น"


def scrape_article_detail_sync(page, article_url: str, article_title: str):
    init_db()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    source_type = classify_source_type(f"{article_title} {article_url}")

    junk_keywords = [
        "download",
        "register",
        "client",
        "button",
        "btn",
        "banner",
        "logo",
        "icon",
        "header",
        "footer",
        "sidebar",
        "menu",
        "qr",
        "qrcode",
        "truemoney",
        "promptpay",
        "facebook",
        "line",
        "share",
        "itemcode",
        "topup_btn",
        "ปุ่ม",
        "แบนเนอร์",
        "สแกน",
    ]

    saved_count = 0
    try:
        page.goto(article_url, wait_until="domcontentloaded", timeout=25000)
        page.wait_for_timeout(1000)

        images = page.query_selector_all(
            ".entry-content table img, .post-content table img, article table img"
        )
        if not images:
            images = page.query_selector_all(
                ".entry-content img, .post-content img"
            )

        item_count = 0
        for img in images:
            src = img.get_attribute("src") or img.get_attribute("data-src")
            if not src or ".gif" in src.lower():
                continue

            src_lower = src.lower()
            if any(j in src_lower for j in junk_keywords):
                continue

            full_img_url = urllib.parse.urljoin(article_url, src)

            right_col_info = None
            try:
                right_col_info = img.evaluate("""el => {
                    let td = el.closest('td');
                    if (td && td.nextElementSibling) {
                        return td.nextElementSibling.innerText.trim();
                    }
                    if (td) return td.innerText.trim();
                    return "";
                }""")
            except:
                pass

            names_list = []
            genders_found = set()
            categories_found = set()

            if right_col_info and len(right_col_info) > 2:
                lines = [
                    line.strip()
                    for line in right_col_info.split("\n")
                    if line.strip()
                ]
                for line in lines:
                    if "ผู้ชาย" in line or "ชาย" in line:
                        genders_found.add("ชาย")
                        continue
                    elif "ผู้หญิง" in line or "หญิง" in line:
                        genders_found.add("หญิง")
                        continue

                    if line.startswith("(") and line.endswith(")"):
                        cat = classify_category(line)
                        if cat != "ชุดแฟชั่น":
                            categories_found.add(cat)
                        continue

                    if len(line) >= 3 and not any(
                        j in line.lower() for j in junk_keywords
                    ):
                        if line not in names_list:
                            names_list.append(line)
                            cat = classify_category(line)
                            if cat != "ชุดแฟชั่น":
                                categories_found.add(cat)

            if names_list:
                final_name = " / ".join(names_list)
            else:
                alt = (
                    img.get_attribute("alt") or img.get_attribute("title") or ""
                ).strip()
                if not alt or alt == article_title:
                    item_count += 1
                    final_name = f"{article_title} (ไอเทมชุดที่ {item_count})"
                else:
                    final_name = alt

            if "ชาย" in genders_found and "หญิง" in genders_found:
                final_gender = "ชาย/หญิง"
            elif "ชาย" in genders_found:
                final_gender = "ชาย"
            elif "หญิง" in genders_found:
                final_gender = "หญิง"
            else:
                final_gender = "ทั้งหมด"

            if len(categories_found) == 1:
                final_category = list(categories_found)[0]
            elif len(categories_found) > 1:
                final_category = "เซตเสื้อผ้า"
            else:
                final_category = classify_category(final_name)

            cursor.execute(
                """
                INSERT OR REPLACE INTO items (name, category, source_type, source_detail, gender, image_url, source_url)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    final_name,
                    final_category,
                    source_type,
                    article_title,
                    final_gender,
                    full_img_url,
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


def run_full_scraper_sync(loop):
    total_saved = 0
    categories = [
        "https://audition.playpark.com/th-th/category/news/promotion/",
        "https://audition.playpark.com/th-th/category/news/event/",
    ]

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for base_url in categories:
            page_num = 1
            cat_name = "Promotion" if "promotion" in base_url else "Event"

            while True:
                target_url = (
                    base_url
                    if page_num == 1
                    else f"{base_url}page/{page_num}/"
                )

                asyncio.run_coroutine_threadsafe(
                    progress_queue.put({
                        "status": "progress",
                        "category": cat_name,
                        "page": page_num,
                        "message": f"กำลังสแกนหมวด {cat_name} หน้าที่ {page_num}...",
                        "total_items": total_saved,
                    }),
                    loop,
                )

                try:
                    response = page.goto(
                        target_url, wait_until="domcontentloaded", timeout=30000
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
                        saved = scrape_article_detail_sync(page, href, title)
                        total_saved += saved
                        asyncio.run_coroutine_threadsafe(
                            progress_queue.put({
                                "status": "progress",
                                "category": cat_name,
                                "page": page_num,
                                "message": f"ดึงข้อมูล: {title[:25]}... (+{saved})",
                                "total_items": total_saved,
                            }),
                            loop,
                        )

                    page_num += 1
                except Exception as e:
                    print(f"⚠️ Error: {e}")
                    break

        browser.close()

    asyncio.run_coroutine_threadsafe(
        progress_queue.put({
            "status": "completed",
            "message": "สแกนข้อมูลครบทุกหน้าสมบูรณ์!",
            "total_items": total_saved,
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
    init_db()
    conn = sqlite3.connect(DB_NAME)
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

    # แก้ไขการสลับเรียงลำดับ: desc สั่ง ASC เพื่อดึงรายการใหม่ขึ้นก่อนตามโครงสร้าง ID ของ DB
    order_direction = "ASC" if sort.lower() == "desc" else "DESC"

    offset = (page - 1) * limit
    query += f" ORDER BY id {order_direction} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return {
        "items": [
            {
                "id": r[0],
                "name": r[1],
                "category": r[2],
                "sourceType": r[3],
                "detail": r[4],
                "gender": r[5],
                "img": r[6],
                "url": r[7],
            }
            for r in rows
        ],
        "total": total_items,
        "page": page,
        "totalPages": (total_items + limit - 1) // limit,
    }


@app.get("/api/scrape-stream")
async def scrape_stream():
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, run_full_scraper_sync, loop)

    async def event_generator():
        while True:
            data = await progress_queue.get()
            yield f"data: {json.dumps(data, ensure_ascii=False)}\n\n"
            if data.get("status") == "completed":
                break

    return StreamingResponse(event_generator(), media_type="text/event-stream")