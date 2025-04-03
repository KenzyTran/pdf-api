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
