from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import pdfplumber
import pandas as pd
import requests
from datetime import datetime
import tempfile

app = FastAPI()

# Pydantic model để nhận JSON body
class PDFRequest(BaseModel):
    pdf_url: str

@app.post("/process-pdf")
def process_pdf(data: PDFRequest):
    try:
        pdf_url = data.pdf_url

        # Tải file PDF về
        response = requests.get(pdf_url)
        if response.status_code != 200:
            return JSONResponse(status_code=400, content={"error": "Không thể tải file PDF từ URL."})

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
            tmp_pdf.write(response.content)
            tmp_pdf_path = tmp_pdf.name

        # Đọc bảng từ PDF
        with pdfplumber.open(tmp_pdf_path) as pdf:
            all_data = []
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    all_data.extend(table)

        if not all_data or len(all_data) < 2:
            return JSONResponse(status_code=400, content={"error": "Không tìm thấy bảng hợp lệ trong PDF."})

        df = pd.DataFrame(all_data[1:], columns=all_data[0])

        # Tách dòng có \n trong ô đầu
        new_data = []
        for _, row in df.iterrows():
            if isinstance(row.iloc[0], str) and '\n' in row.iloc[0]:
                for r in row.iloc[0].split('\n'):
                    new_data.append(r.split())
            else:
                new_data.append(row.tolist())

        df_cleaned = pd.DataFrame(new_data, columns=df.columns)

        # Lọc dữ liệu
        df_cleaned = df_cleaned[~df_cleaned.apply(lambda row: row.astype(str).str.contains('Tổng', case=False).any(), axis=1)]
        df_cleaned = df_cleaned[df_cleaned.iloc[:, 1].apply(lambda x: isinstance(x, str) and len(x.strip()) > 1)]

        df_final = pd.DataFrame()
        df_final['MA_CK'] = df_cleaned.iloc[:, 1]
        df_final['SLCP_SOHUU'] = df_cleaned.iloc[:, 6]
        df_final['ROOM_CON_LAI'] = df_cleaned.iloc[:, 7]

        # Xử lý định dạng số
        df_final['SLCP_SOHUU'] = df_final['SLCP_SOHUU'].astype(str).str.replace('.', '', regex=False)
        df_final['ROOM_CON_LAI'] = df_final['ROOM_CON_LAI'].astype(str).str.replace('.', '', regex=False)
        df_final['SLCP_SOHUU'] = pd.to_numeric(df_final['SLCP_SOHUU'], errors='coerce')
        df_divisor = pd.to_numeric(df_cleaned.iloc[:, 3].astype(str).str.replace('.', '', regex=False), errors='coerce')
        df_final['DIVISOR'] = df_divisor.values

        # Hàm xử lý chia
        def calc_percent_sohuu(row):
            sohhuu = row['SLCP_SOHUU']
            divisor = row['DIVISOR']
            if pd.isnull(sohhuu) or pd.isnull(divisor):
                return ''
            if sohhuu == 0 and divisor == 0:
                return ''
            if divisor == 0:
                return ''
            value = sohhuu / divisor
            return f"{value:,.5f}"

        df_final['PHAN_TRAM_SO_HUU'] = df_final.apply(calc_percent_sohuu, axis=1)
        df_final = df_final[['MA_CK', 'SLCP_SOHUU', 'PHAN_TRAM_SO_HUU', 'ROOM_CON_LAI']]

        # Ghi file Excel tạm
        file_date = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_excel:
            df_final.to_excel(tmp_excel.name, index=False)
            tmp_excel_path = tmp_excel.name

        return FileResponse(
            tmp_excel_path,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=f"{file_date}.xlsx"
        )

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
