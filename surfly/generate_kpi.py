#!/usr/bin/env python
# coding: utf-8

# In[235]:
import psycopg2
import matplotlib.pyplot as plt
import csv


# In[243]:


def churn_rate():
    command_churn_rate_kpi="""
    WITH cte AS     
        (SELECT 
             to_char(created_at, 'YYYY-MM') as month,     
             COUNT(DISTINCT company_id) as company_count     
             FROM session     
             GROUP BY to_char(created_at, 'YYYY-MM')     
             ORDER BY 1 
        ) , 
    cte2 AS    
        (SELECT 
             month, 
             company_count, 
             NULLIF(LEAD(company_count,1) OVER (ORDER BY month),0) next_month_company_count 
             from cte
        )     
     SELECT 
         DISTINCT month,     
         ((cast(company_count as decimal) - next_month_company_count) /company_count) as monthly_churn_rate 
     FROM cte2"""

    conn = None

    try:
            conn = psycopg2.connect(
                        host="postgres_db",
                        database="postgres",
                        user="postgres",
                        password="postgres",
                        port=5432)

            cur = conn.cursor()

            cur.execute(command_churn_rate_kpi)

            records = cur.fetchall()
            
            month = []
            churn_rate = []

            for row in records:
                month.append(row[0])
                churn_rate.append(row[1])
                
            plt.plot(month,churn_rate,'-')
            
            plt.xticks(rotation=45)
            
            plt.title('Monthly Churn Rate')
            plt.xlabel('Year-Month')
            plt.ylabel('Churn Rate')
            
            plt.savefig('monthly churn rate.jpg')
            
            plt.show()

            for row in records:
                print("month = ", row[0], )
                print("monthly churn rate = ", row[1],  "\n")

            cur.close()

            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
                print(error)
    finally:
                if conn is not None:
                    conn.close()


# In[244]:


churn_rate()


# In[245]:


def average_revenue():
    command_monthly_avg_revenue_per_customer=""" 
        WITH cte AS     
        (SELECT 
             to_char(created_at, 'YYYY-MM') as month,
             cc.company_size,
             cc.subscription_fee,
             count(DISTINCT s.company_id) as customer_count
             FROM session s
             inner join company c
             on s.company_id=c.ID
             INNER JOIN company_category cc
             on c.company_category_id=cc.id
             GROUP BY to_char(created_at, 'YYYY-MM'),cc.subscription_fee, cc.company_size
        ),
        cte2 AS 
         (SELECT month, (subscription_fee*customer_count) revenue, customer_count
         from cte
        )
        SELECT month, sum(revenue)/sum(customer_count) as monthly_average_revenue_per_customer
        from cte2
        group by month
        order by month
        """   

    conn = None

    try:
            conn = psycopg2.connect(
                        host="postgres_db",
                        database="postgres",
                        user="postgres",
                        password="postgres",
                        port=5432)

            cur = conn.cursor()

            cur.execute(command_monthly_avg_revenue_per_customer)

            month = []
            avg_revenue_per_customer = []
            
            records = cur.fetchall()
            
            for row in records:
                month.append(row[0])
                avg_revenue_per_customer.append(row[1])
                
            plt.plot_date(month,avg_revenue_per_customer,'-')
            
            plt.xticks(rotation=45)
            
            plt.title('Monthly Average Revenue Per Customer')
            plt.xlabel('Year-Month')
            plt.ylabel('Average Ravenue Per Customer (€)')
            
            plt.savefig('monthly_average_revenue_per_customer.jpg')
            
            plt.show()

            for row in records:
                print("month= ", row[0],)
                print("avg_revenue_per_customer= ", row[1],  "\n")

            cur.close()

            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
                print(error)
    finally:
                if conn is not None:
                    conn.close()


# In[246]:


def revenue_ratio():
    command_revenue_ratio="""
    WITH cte AS     
        (SELECT 
             to_char(created_at, 'YYYY-MM') as month,
             cc.company_size,
             cc.subscription_fee,
             count(DISTINCT s.company_id) as customer_count
             FROM session s
             INNER join company c
             on s.company_id=c.ID
             INNER JOIN company_category cc
             on c.company_category_id=cc.id
             GROUP BY to_char(created_at, 'YYYY-MM'),cc.subscription_fee, cc.company_size
        )
        SELECT month, company_size,
         (subscription_fee*customer_count)/
         sum(subscription_fee*customer_count) over(partition by month order by month) revenue
         from cte
    """    
    
    conn = None

    try:
            conn = psycopg2.connect(
                        host="postgres_db",
                        database="postgres",
                        user="postgres",
                        password="postgres",
                        port=5432)

            cur = conn.cursor()

            cur.execute(command_revenue_ratio)
            
            month = []
            revenue_ratio_according_to_company_size = []
            
            month2 = []
            revenue_ratio_according_to_company_size2 = []

            records = cur.fetchall()
            
            for row in records:
                if row[1]=='Small':
                    month.append(row[0])
                    revenue_ratio_according_to_company_size.append(row[2])
                else:
                    month2.append(row[0])
                    revenue_ratio_according_to_company_size2.append(row[2])
                
            plt.plot(month,revenue_ratio_according_to_company_size,'g',label="Small Companies")
            
            plt.plot(month,revenue_ratio_according_to_company_size2,'r',label="Large Companies")
            
            plt.xticks(rotation=45)
            
            plt.title('Monthly Revenue Ratio According to Company Size')
            plt.xlabel('Year-Month')
            plt.ylabel('Revenue Ratio')
            
            plt.legend()
            
            plt.savefig('monthly_revenue_ratio_according_to_company_size.jpg')
            
            plt.show()
                
            for row in records:
                print("month = ", row[0], )
                print("company_size = ", row[1], )
                print("revenue_ratio_according_to_company_size= ", row[2],  "\n")


            cur.close()

            conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
                print(error)
    finally:
                if conn is not None:
                    conn.close()


# In[247]:


def main():
    churn_rate()
    average_revenue()
    revenue_ratio()
    


# In[248]:


if __name__ == "__main__":
    main()


# In[ ]:




