from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import pdfplumber
import pandas as pd
import requests
from datetime import datetime
import tempfile
import os
from urllib.parse import urlparse
import base64

from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import tempfile, time, os, re, unicodedata, zipfile

app = FastAPI()

# Pydantic model để nhận JSON body
class PDFRequest(BaseModel):
    pdf_url: str

@app.post("/process-pdf")
def process_pdf(data: PDFRequest):
    pdf_url = data.pdf_url

    try:
        # Tải file PDF từ URL về tạm
        response = requests.get(pdf_url)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Không tải được file PDF từ URL")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(response.content)
            tmp_pdf_path = tmp_pdf.name

        # Xử lý PDF
        with pdfplumber.open(tmp_pdf_path) as pdf:
            all_data = []
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    all_data.extend(table)

        df = pd.DataFrame(all_data[1:], columns=all_data[0])

        new_data = []
        stop_adding = False

        for index, row in df.iterrows():
            if stop_adding:
                break
            if isinstance(row.iloc[0], str) and 'SÀN ĐẠI CHÚNG CHƯA NIÊM YẾT' in row.iloc[0]:
                stop_adding = True
                break
            if isinstance(row.iloc[0], str) and '\n' in row.iloc[0]:
                rows = row.iloc[0].split('\n')
                for r in rows:
                    new_row = r.split()
                    new_data.append(new_row)
            else:
                new_data.append(row.tolist())

        df_cleaned = pd.DataFrame(new_data, columns=df.columns)
        df_cleaned = df_cleaned[~df_cleaned['STT'].isin(['STT', 'SÀN'])]
        df_cleaned = df_cleaned[df_cleaned['Mã CK'] != '2']

        df_final = df_cleaned.iloc[:, [1, 4, 5, 6]].copy()
        df_final.columns = ['MA_CK', 'SLCP_SOHUU', 'PHAN_TRAM_SO_HUU', 'ROOM_CON_LAI']

        df_final['SLCP_SOHUU'] = df_final['SLCP_SOHUU'].replace('', '0')
        df_final['ROOM_CON_LAI'] = df_final['ROOM_CON_LAI'].replace('', '0')

        df_final['SLCP_SOHUU'] = df_final['SLCP_SOHUU'].str.replace('.', '', regex=False).astype(float)
        df_final['ROOM_CON_LAI'] = df_final['ROOM_CON_LAI'].str.replace('.', '', regex=False).astype(float)

        df_final['PHAN_TRAM_SO_HUU'] = df_final['PHAN_TRAM_SO_HUU'].str.replace('%', '', regex=False)
        df_final['PHAN_TRAM_SO_HUU'] = pd.to_numeric(df_final['PHAN_TRAM_SO_HUU'], errors='coerce')
        df_final = df_final.dropna(subset=['PHAN_TRAM_SO_HUU'])

        df_final['PHAN_TRAM_SO_HUU'] = df_final['PHAN_TRAM_SO_HUU'] / 100
        df_final['PHAN_TRAM_SO_HUU'] = df_final['PHAN_TRAM_SO_HUU'].apply(lambda x: f"{x:,.5f}")

        today = datetime.now().strftime('%m/%d/%Y')
        df_final.insert(0, 'NGAY', today)

        # Lưu ra file Excel tạm
        file_date = datetime.now().strftime('%Y-%m-%d')
        tmp_excel_path = os.path.join(tempfile.gettempdir(), f"{file_date}.xlsx")
        df_final.to_excel(tmp_excel_path, index=False)

        return FileResponse(
            tmp_excel_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"{file_date}.xlsx"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Model nhận vào mã chứng khoán và kỳ báo cáo
class CrawlRequest(BaseModel):
    stock_code: str   # ví dụ "PVS"
    period:     str   # ví dụ "Q4.2024", "H1.2024", "Y2024"


def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


def wait_for_element(driver, by, selector, timeout=20):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((by, selector))
    )


def strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize('NFD', text)
    return ''.join(ch for ch in nfkd if unicodedata.category(ch) != 'Mn')


def extract_quarter_year(report_name: str) -> str:
    """
    Trích:
      - Báo cáo bán niên YYYY  → H1.YYYY
      - Báo cáo năm YYYY      → YYYYY
      - Quý N[/ ]YYYY hoặc Quý N năm YYYY → QN.YYYY
    """
    name = strip_accents(report_name.lower())
    # 1) Bán niên
    m = re.search(r'ban nien\s*(\d{4})', name)
    if m:
        return f"H1.{m.group(1)}"
    # 2) Annual
    m = re.search(r'\bnam\s*(\d{4})', name)
    if m:
        return f"Y{m.group(1)}"
    # 3) Quý
    for p in (r'\bquy\s*(\d+)[/ ]*(\d{4})', r'\bquy\s*(\d+)[/ ]*nam\s*(\d{4})'):
        m = re.search(p, name)
        if m:
            return f"Q{m.group(1)}.{m.group(2)}"
    return "unknown"


def get_table_data(soup, selector):
    rows = soup.select(selector)
    data = []
    for tr in rows:
        tds = tr.find_all("td")
        row = [td.find("span").get_text(strip=True) if td.find("span") else "" for td in tds]
        if row:
            data.append(row)
    return data


def get_table_headers(driver, header_id):
    header_table = driver.find_element(By.ID, header_id)
    header_html = header_table.get_attribute("outerHTML")
    soup = BeautifulSoup(header_html, "html.parser")
    header_tr = soup.select_one("tbody > tr:nth-of-type(2)")
    return [th.get_text(strip=True) for th in header_tr.find_all("th")]


def click_tab(driver, tab_id):
    tab = driver.find_element(By.ID, tab_id)
    driver.execute_script("arguments[0].scrollIntoView(true);", tab)
    time.sleep(1)
    driver.execute_script("arguments[0].click();", tab)
    time.sleep(2)


def process_table_data(driver, header_id, data_id, tab_name, report_dir, id_mack):
    headers = get_table_headers(driver, header_id)
    div = driver.find_element(By.ID, data_id)
    soup = BeautifulSoup(div.get_attribute("outerHTML"), "html.parser")
    data = get_table_data(soup, "tbody > tr")
    df = pd.DataFrame(data, columns=headers)
    path = os.path.join(report_dir, f"{id_mack}_{tab_name}.csv")
    df.to_csv(path, index=False, encoding='utf-8-sig')


def process_report_detail(driver):
    wait_for_element(driver, By.ID, "pt2:pt1::tabbc")
    time.sleep(2)
    # Lấy mã chứng khoán làm id_mack
    mdn_value = driver.find_element(By.CSS_SELECTOR, "td.xth.xtk").text.strip()
    id_mack = re.sub(r'[\\/*?:"<>|]', "_", mdn_value)
    # Tạo thư mục tạm cho báo cáo này
    report_dir = tempfile.mkdtemp(prefix=f"{id_mack}_")
    # Xử lý các bảng
    process_table_data(driver, "pt2:t2::ch::t", "pt2:t2::db", "CDKT", report_dir, id_mack)
    click_tab(driver, "pt2:KQKD::disAcr")
    process_table_data(driver, "pt2:t3::ch::t", "pt2:t3::db", "KQKD", report_dir, id_mack)
    click_tab(driver, "pt2:LCTT-TT::disAcr")
    process_table_data(driver, "pt2:t5::ch::t", "pt2:t5::db", "LCTT-TT", report_dir, id_mack)
    click_tab(driver, "pt2:LCTT-GT::disAcr")
    process_table_data(driver, "pt2:t6::ch::t", "pt2:t6::db", "LCTT-GT", report_dir, id_mack)
    return report_dir


def get_report_links(driver):
    reports = []
    table = wait_for_element(driver, By.ID, "pt9:t1::db")
    links = table.find_elements(By.CSS_SELECTOR, "a[id$=':cl1']")
    for el in links:
        name = el.text.strip()
        # Bỏ qua report của công ty mẹ/riêng
        if any(k in name.lower() for k in ('mẹ', 'riêng')):
            continue
        link_id = el.get_attribute("id")
        idx = int(link_id.split(":")[2])
        qy = extract_quarter_year(name)
        reports.append((idx, link_id, name, qy))
    return reports

@app.post("/crawl-report")
def crawl_report(data: CrawlRequest):
    driver = setup_driver()
    try:
        # 1) Search mã
        driver.get("https://congbothongtin.ssc.gov.vn/faces/NewsSearch")
        inp = wait_for_element(driver, By.ID, "pt9:it8112::content")
        inp.clear(); inp.send_keys(data.stock_code)
        btn = driver.find_element(By.XPATH, "//span[text()='Tìm kiếm']/ancestor::a")
        driver.execute_script("arguments[0].click();", btn)
        wait_for_element(driver, By.ID, "pt9:t1::db")
        time.sleep(2)
        # 2) Lấy report list và tìm đúng kỳ
        reports = get_report_links(driver)
        match = next((r for r in reports if r[3] == data.period), None)
        if not match:
            raise HTTPException(status_code=404, detail=f"No report for period {data.period}")
        _, link_id, _, _ = match
        # 3) Click detail
        el = driver.find_element(By.ID, link_id)
        driver.execute_script("arguments[0].scrollIntoView(true);", el)
        time.sleep(0.5)
        el.click()
        time.sleep(2)
        # 4) Xuất CSV
        report_dir = process_report_detail(driver)
        # 5) Zip và trả file
        zip_path = os.path.join(tempfile.gettempdir(), f"{data.stock_code}_{data.period}.zip")
        with zipfile.ZipFile(zip_path, 'w') as zf:
            for root, _, files in os.walk(report_dir):
                for fn in files:
                    if fn.lower().endswith('.csv'):
                        full = os.path.join(root, fn)
                        zf.write(full, arcname=fn)
        return FileResponse(zip_path, media_type="application/zip", filename=os.path.basename(zip_path))

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        driver.quit()
