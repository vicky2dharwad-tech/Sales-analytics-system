Sales Analytics System - Windows Installation Guide



Repository Structure

sales-analytics-system/

├── README.md ← You are here

├── main.py ← RUN THIS FILE

├── requirements.txt ← Python dependencies

├── utils/ ← Core program modules

│ ├── file\_handler.py ← Part 1: File handling

│ ├── data\_processor.py ← Part 2: Data analysis

│ └── api\_handler.py ← Part 3: API integration

├── data/ ← Input data folder

│ └── sales\_data.txt ← Provided sales data (80 records)

└── output/ ← Generated files (auto-created)



&nbsp;Quick Start for Windows



Step 1: Open Command Prompt as Administrator

1\. Press `Windows Key + X`

2\. Select Windows Terminal (Admin) or Command Prompt (Admin)

3\. Click \*\*Yes\*\* if asked for permission



Step 2: Navigate to Project Folder\*\*

cmd

cd C:\\Users\\\[YourUsername]\\sales-analytics-system

Replace \[YourUsername] with your actual Windows username



OR if you saved it on Desktop:



cmd

cd C:\\Users\\\[YourUsername]\\Desktop\\sales-analytics-system

Step 3: Install Required Package

cmd

pip install requests

If pip fails, try:



cmd

python -m pip install requests

If Python not found:



cmd

py -m pip install requests

Step 4: Run the Program

cmd

python main.py

