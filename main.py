from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel
import pdfplumber
import pandas as pd
import requests
from datetime import datetime
import tempfile
import os

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

@app.post("/split-xlsx")
async def split_xlsx(data: dict):
    try:
        # Lấy URL từ request body
        xlsx_url = data.get("xlsx_url")
        if not xlsx_url:
            raise HTTPException(status_code=400, detail="Thiếu trường 'xlsx_url' trong request body")

        # Trích xuất tên file gốc từ URL
        parsed_url = urlparse(xlsx_url)
        file_name = os.path.basename(parsed_url.path)  # Lấy tên file từ URL
        file_name_without_extension = os.path.splitext(file_name)[0]  # Loại bỏ phần mở rộng (.xlsx)

        # Tải file từ URL về server tạm thời
        response = requests.get(xlsx_url)
        if response.status_code != 200:
            raise HTTPException(status_code=400, detail="Không tải được file từ URL")

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(response.content)
            tmp_file_path = tmp_file.name

        # Đọc file Excel và lấy danh sách các sheet
        excel_data = pd.ExcelFile(tmp_file_path)
        sheet_names = excel_data.sheet_names

        # Tạo danh sách các file đầu ra dưới dạng base64
        files_base64 = []
        for sheet_name in sheet_names:
            df = excel_data.parse(sheet_name)  # Đọc sheet thành DataFrame

            # Kết hợp tên file gốc và tên sheet để tạo tên file mới
            output_filename = f"{file_name_without_extension}_{sheet_name}.xlsx"

            # Lưu DataFrame thành file tạm thời
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_output_file:
                df.to_excel(tmp_output_file.name, index=False)

                # Đọc nội dung file và mã hóa sang base64
                with open(tmp_output_file.name, "rb") as file:
                    file_content = file.read()
                    file_base64 = base64.b64encode(file_content).decode("utf-8")

            # Thêm vào danh sách kết quả
            files_base64.append({
                "filename": output_filename,
                "content": file_base64
            })

        # Trả về danh sách các file dưới dạng base64
        return JSONResponse(
            content={
                "message": "Tách file thành công",
                "files": files_base64
            },
            status_code=200
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
