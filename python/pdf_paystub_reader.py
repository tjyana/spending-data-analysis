# import pdfplumber
# import pandas as pd

# rows = []

# with pdfplumber.open("statement.pdf") as pdf:
#     for page in pdf.pages:
#         table = page.extract_table()
#         if not table:
#             continue
#         header, *data = table
#         for row in data:
#             # map row columns to your standard schema
#             # e.g. date, description, amount, etc.
#             rows.append(row)

# df = pd.DataFrame(rows, columns=["date_raw", "description_raw", "amount_raw", "etc"])
# # do cleaning here: parse dates, amounts, sign, etc.


import pdfplumber
import pandas as pd

input_pdf = "input.pdf"
output_csv = "output.csv"

rows = []

with pdfplumber.open(input_pdf) as pdf:
    for page_number, page in enumerate(pdf.pages, start=1):
        text = page.extract_text()
        if text:
            for line in text.split("\n"):
                rows.append([page_number, line])

df = pd.DataFrame(rows, columns=["page", "text"])
df.to_csv(output_csv, index=False)

print(f"Done! Extracted {len(rows)} lines into {output_csv}")
