# Real-time Loan Fraud Detection from Streaming Card and Loan Data
โปรเจคนี้จะ detect ข้อมูลแบบ real-time จากสองที่มาคือ Card data และ Loan data ซึ่งจะทำการ generate 
ข้อมูลและส่งไปที่ topic ที่สร้างไว้ 2 topics คือ Card topic และ Loan topic โดยใช้บริการ global real-time messaging 
ของ google cloud platform และสร้าง subscription เพื่อรับข้อมูลที่ได้ generate ออกมา 2 topics คือ Card topic 
และ Loan topic เช่นกัน เพื่อทำการประมวลผลตาม Requirements ที่ได้กำหนดไว้เพื่อตรวจจับการโฉ้โกงเงินกู้และบันทึกลงใน Big query
เพื่อนำไปวิเคราะห์ข้อมูลต่อไป 

# ตัวอย่างข้อมูลทีทำการ Generate ขึ้น
ข้อมูลที่ Generate ทำการ Generate ขึ้นจาก 2 ที่ซึ่ง Generate จากไฟล์ Generate_loan_data.py คือ \
1.Card data ซึ่งมีจำนวน 8 Columns คือ \
1.1 Customer_id ซึ่งคือ unique identification ของเจ้าของบัตร \
1.2 Relationship_no คือ Relationship number of Card \
1.3 Card_type คือประเภทของบัตรซึ่งมี 8 ประเภทคือ 1.Issuers 2.AccountNow_Prepaid_Cards 3.MasterCard 4.Discover 5.Capital_One 6.American_Express 7.Network 8.Visa \
1.4 Max_credit_limit คือ max spent limit ของ customer ใน 1 เดือน \
1.5 Total_Spent คือ ค่าใช้จ่ายของ customer ใน 1 เดือน \
1.6 Cash_withdrwn คือ จำนวนเงินที่ customer ถอน ใน 1 เดือนจากตู้ ATM ซึ่งรวมอยู่ใน Total_Spent เรียบร้อย \
1.7 Cleared_amount คือ การชำระค่าใช้จ่ายของ customer ใน 1 เดือน\
1.8 Last_date คือ วันสุดท้ายของเดือนและเป็นวัยสุดท้ายในการชำระเงินนั่นเอง  

