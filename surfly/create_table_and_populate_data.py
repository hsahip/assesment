#!/usr/bin/env python
# coding: utf-8

# In[52]:


import psycopg2
from faker import Faker
from datetime import datetime
from random import randint
from dateutil.relativedelta import relativedelta


# In[126]:


def create_tables():
    
    print("Table creation has started!")
    
    commands = (
        """
        CREATE TABLE IF NOT EXISTS company_category (
              id SERIAL PRIMARY KEY,
              company_size varchar(5) NOT NULL, 
              subscription_fee decimal(12,2) NOT NULL,
              number_of_sessions SMALLINT NOT NULL
            )
        """,
        """ 
        CREATE TABLE IF NOT EXISTS company (
              id SERIAL PRIMARY KEY,
              company_name varchar(100),
              company_category_id INT NOT NULL REFERENCES company_category(id),
              became_company_date TIMESTAMP
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS session (
              id SERIAL PRIMARY KEY,
              company_id INT NOT NULL REFERENCES company(id),
              created_at TIMESTAMP
            )
        """)
    
    conn = None
    try:
        conn = psycopg2.connect(
                    host="postgres_db",
                    database="postgres",
                    user="postgres",
                    password="postgres",
                    port=5432)
        
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)
   
        cur.close()

        conn.commit()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Table cretion error! :",error)
    finally:
        if conn is not None:
            conn.close()


# In[12]:


def populate_company_category_data():
    
    print("Dummy data population for the company_category table has started!")
    
    conn = None
    try:
        conn = psycopg2.connect(
                    host="postgres_db",
                    database="postgres",
                    user="postgres",
                    password="postgres",
                    port=5432)
        
        cur = conn.cursor()
        
        company_category_insert ="""INSERT INTO company_category(company_size,subscription_fee,number_of_sessions) 
                    VALUES ('Small',19,5)
                    ,('Large',99,10)"""
        
        cur.execute(company_category_insert)
   
        cur.close()

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Company_category table data population Error! : ",error)
    finally:
        if conn is not None:
            conn.close()


# In[20]:


def populate_company_data():
    
    fake=Faker()
    
    print("The Dummy data population for the company table has started!")
    
    select_Query ="""SELECT
            $s,
            CASE WHEN RANDOM() <= 0.3 
                THEN 2
                ELSE 1 
            END, 
            md5(RANDOM()::TEXT)
            FROM generate_series(1, 500);""" 
    
    conn = None
    try:
        conn = psycopg2.connect(
                    host="postgres_db",
                    database="postgres",
                    user="postgres",
                    password="postgres",
                    port=5432)
        
        cur = conn.cursor()
        
        date_time_start = datetime.strptime('2020-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
        date_time_end = datetime.strptime('2020-01-31 23:59:59','%Y-%m-%d %H:%M:%S')
    
        
        for i in range (500):
            company_name =fake.company()
            
            became_company_date=fake.date_time_between(date_time_start, date_time_end)

            cur.execute("INSERT INTO COMPANY (company_name,company_category_id,became_company_date)             VALUES (%s, 1, %s) ",(company_name,became_company_date));
            
        company_category_update = "update company set company_category_id=2                       where id in (select id from company LIMIT (Select count(*)*0.3 from company))"

        cur.execute(company_category_update)
 
        cur.close()

        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Company table data population Error! : ",error)
    finally:
        if conn is not None:
            conn.close()
    


# In[124]:


def populate_session_data():
    
    fake=Faker()

    date_time_start = datetime.strptime('2020-01-01 00:00:00','%Y-%m-%d %H:%M:%S')

    month=1
    
    #populate data for each month 1,12
    for month in range(1, 13):
        date_time_end= date_time_start+ relativedelta(months=1, days=-1)
        
        conn = None
        try:
            conn = psycopg2.connect(
                        host="postgres_db",
                        database="postgres",
                        user="postgres",
                        password="postgres",
                        port=5432)

            cur = conn.cursor()       
            
            #get company list
            company_list = "select id from company"
            
            cur.execute(company_list)
            
            companies = cur.fetchall()
            
            #populate data for each company
            for company_id in companies:
                
                max_session_string = "select comp.id,                 cc.company_size, cc.subscription_fee, cc.number_of_sessions from                 company comp join company_category cc on comp.company_category_id=cc.id where comp.id =%s"             
                                       
                cur.execute(max_session_string, [company_id])
                
                records = cur.fetchall()

                for row in records:
                    max_session=row[3]
                    session_count = randint(0,max_session)
                    
                if month > 1: #Other Months      
                    #Get previous session count for the company id
                    prev_sess_count_string= "select count(id) from session                         where company_id=%s and created_at between %s and %s"
                    
                    cur.execute(prev_sess_count_string,(company_id, (date_time_start+ relativedelta(months=-1))
                               ,(date_time_end + relativedelta(months=-1))))
                    
                    previous_session_count= cur.fetchall()                    
                    
                    for row in previous_session_count:
                        #check if the previous session count is greater than 0 for the company id
                        #if yes, populate new session for the company for next month
                        if row[0] > 0:
                            for i in range(session_count):                                                
                                created_at=fake.date_time_between(date_time_start, date_time_end)                          
                                insert_script="insert into session(company_id , created_at) values (%s, %s)"   
                                cur.execute(insert_script,(company_id, created_at)) 
                    
                else: #First month      
                    #Populate sessions to all companies for only first month
                    for i in range(session_count): 
                        created_at=fake.date_time_between(date_time_start, date_time_end) 
                        insert_script="insert into session(company_id , created_at) values (%s, %s)"               
                        cur.execute(insert_script,(company_id, created_at))                                            
                        
            cur.close()

            conn.commit()
            
            date_time_start= date_time_start+ relativedelta(months=1)
            
        except (Exception, psycopg2.DatabaseError) as error:
            print("Session table data population Error! :",error)
        finally:
            if conn is not None:
                conn.close()
                
                  
    


# In[ ]:


def main():
    create_tables()
    populate_company_category_data()
    populate_company_data()
    populate_session_data()


# In[ ]:


if __name__ == "__main__":
    main()

