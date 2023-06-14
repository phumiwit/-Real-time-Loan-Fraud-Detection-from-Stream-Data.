# Real-time Loan Fraud Detection from Streaming Card and Loan Data
โปรเจคนี้จะ detect ข้อมูลแบบ real-time จากสองที่มาคือ Card data และ Loan data ซึ่งจะทำการ generate 
ข้อมูลและส่งไปที่ topic ที่สร้างไว้ 2 topics คือ Card topic และ Loan topic โดยใช้บริการ global real-time messaging 
ของ google cloud platform และสร้าง subscription เพื่อรับข้อมูลที่ได้ generate ออกมา 2 topics คือ Card topic 
และ Loan topic เช่นกัน เพื่อทำการประมวลผลตาม Requirements ที่ได้กำหนดไว้เพื่อตรวจจับการโฉ้โกงเงินกู้และบันทึกลงใน Big query
เพื่อนำไปวิเคราะห์ข้อมูลต่อไป 
