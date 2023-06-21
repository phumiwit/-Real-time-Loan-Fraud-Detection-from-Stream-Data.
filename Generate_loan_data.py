import datetime, json, os, random, time

project = 'your project id'

# สุ่มRelationship_no โดยใช้ตัวเลขที่ไม่ซ้ำกัน
def generate_unique_numbers(count):
    numbers = set()
    while len(numbers) < count:
        number = random.randint(1000, 9999)
        numbers.add(number)
    return numbers

unique_numbers = generate_unique_numbers(1024)
Relationship_no = [f"R_{number}" for number in unique_numbers]

# สุ่ม Customer_id ที่ไม่ซ้ำกันจำนวน 1024
Customer_id = []
for i in range(1024):
    if i < 10:
        generated_id = 'CT' + str(i) + '0000'
    elif 10 < i < 100:
        generated_id = 'CT' + str(i) + '000'
    elif i > 100:
        generated_id = 'CT' + str(i) + '00'
    Customer_id.append(generated_id)

# Card type ทั้งหมดที่จะทำการสุ่ม
Card_type1 = ['Issuers','AccountNow_Prepaid_Cards','MasterCard','Discover','Capital_One','American_Express','Network','Visa']
# Max_credit_limit ทั้งหมดที่จะทำการสุ่ม
Max_credit_limit1 = [500,900,1000,600,700,800]
# วันสุดท้ายของเดือนทั้งหมด1 12 เดือน
Last_date = ['31-01-2019', '28-02-2019', '31-03-2019', '30-04-2019','31-05-2019', '30-06-2019', '31-07-2019', '31-08-2019','30-09-2019', '31-10-2019', '30-11-2019', '31-12-2019']

twel_month = 0
Max_credit_limit = random.choice(Max_credit_limit1)
Card_type = random.choice(Card_type1)
Total_Spent = random.randint(1,Max_credit_limit)
Cash_withdrawn = random.randint(10,100)
Cleared_amount = random.randint(1,Max_credit_limit)
month = 0
for i in range(0,1024):
    Total_Spent = random.randint(1,Max_credit_limit)
    Cash_withdrawn = random.randint(10,100)
    Cleared_amount = random.randint(1,Max_credit_limit)
    # ทำการสุ่มทุกๆ 12 เดือน
    if i % 12 == 0:
        Card_type = random.choice(Card_type1)
        Max_credit_limit = random.choice(Max_credit_limit1)
        Cash_withdrawn = random.randint(10,100)
        Total_Spent = random.randint(1,Max_credit_limit)
        Cleared_amount = random.randint(1,Max_credit_limit)
        twel_month += 1
    if month == 12:
        month = 0
    data = {
        'Customer_id':Customer_id[twel_month],
        'Relationship_no':Relationship_no[twel_month],
        'Card_type':Card_type,
        'Max_credit_limit':Max_credit_limit,
        'Total_Spent':Total_Spent,
        'Cash_withdrawn':Cash_withdrawn,
        'Cleared_amount':Cleared_amount,
        'Last_date':Last_date[month]
    }
    month += 1
    message = json.dumps(data)
    # คำสั่งสำหรับ publish ไปที่ topic Card
    command = "gcloud --project={} pubsub topics publish Card --message='{}'".format(project, message)
    print(command)
    os.system(command)
    

# สุ่ม Loan_id โดยใช้ตัวเลขที่ไม่ซ้ำกัน
def generate_unique_numbers1(count):
    numbers = set()
    while len(numbers) < count:
        number = random.randint(1000, 9999)
        numbers.add(number)
    return numbers

unique_numbers = generate_unique_numbers1(324)
Loan_id = [f"LN_{number}" for number in unique_numbers]

# สุ่ม Customer_id ของ Loan จาก Customer_id ของ Card เพื่อใช้เป็น key
count = 324
Customer_id_loan = random.sample(Customer_id, count)

# Customer_category ทั้งหมดที่ใช้ในการสุ่ม
Customer_category1 = ['Serviceman', 'Businessman', 'Student', 'Others', 'Self']

# Due_amount ทั้งหมดที่ใช้ในการสุ่ม
Due_amount1 = [2000, 50000,  1000,  9000,  5000,  7000,  6000]

# Loan_category ทั้งหมดที่ใช้ในการสุ่
Loan_category1 = ['Medical_Loan', 'Personal_Loan', 'Education_Loan']

Due_amount = random.choice(Due_amount1)
Loan_category = random.choice(Loan_category1)
Customer_category = random.choice(Customer_category1)

twel_month = 0
month = 0
due_day = []
payment_day = []

# สุ่ม due_date และ payment_date
for j in range(1, 13):
    due_day1 = random.randint(18, 28)
    payment_day1 = random.randint(18,28)
    
    if j < 10:
        j = '0' + str(j)
            
    due_date =  str(due_day1) + '-' + str(j) + '-'  + '2019'
    payment_date = str(payment_day1) +  '-' + str(j) + '-' + '2019'
    due_day.append(due_date)
    payment_day.append(payment_date)
    
for i in range(0,324):
    # ทำการสุ่มทุกๆ 12 เดือน
    if i % 12 == 0:
        Due_amount = random.choice(Due_amount1)
        Loan_category = random.choice(Loan_category1)
        Customer_category = random.choice(Customer_category1)
        twel_month += 1
    if month == 12:
        month = 0
    data = {
            'Customer_id':Customer_id_loan[twel_month],
            'Customer_category':Customer_category,
            'Load_id': Loan_id[twel_month],
            'Loan_category':Loan_category,
            'Due_date':due_day[month],
            'Due_amount':Due_amount,
            'Payment_date':payment_day[month]
           
        }
    month += 1
    message = json.dumps(data)
    # คำสั่งสำหรับ publish ไปที่ topic Loan
    command = "gcloud --project={} pubsub topics publish Loan --message='{}'".format(project, message)
    print(command)
    os.system(command)
 