import random 
import datetime
import psycopg2
import threading 
import time

#define trade_data list with starting prices
trade_data = [('GOOG',107), ('MSFT',308),('AMZN',105),('CRM',197),('AAPL',173),('TSLA',170),('NFLX',330),('META',233),('NVDA',291),('NIKE',126)]

#connect to postgres database
conn = psycopg2.connect(
        host="localhost",
        port="8041",
        database="postgres",
        user="postgres",
        password="password"
)

#create cursor object
cur = conn.cursor()

#create the trade table on timescale
cur.execute("""
    CREATE TABLE IF NOT EXISTS trade (
        timestamp TIMESTAMP NOT NULL,
        sym TEXT NOT NULL,
        price FLOAT NOT NULL,
        volume INTEGER NOT NULL,
        side TEXT NOT NULL
    )
""")

# Convert the market_data table to a hypertable with partitioning on the timestamp column

cur.execute("SELECT create_hypertable('trade','timestamp');");

# Commit the changes to the database
conn.commit()

# Close the database connection
cur.close()
conn.close()

def tradebatch(batchlength):

    #connect to postgres database
    conn = psycopg2.connect(
            host="localhost",
            port="8041",
            database="postgres",
            user="postgres",
            password="password"
    )

    #create cursor object
    cur = conn.cursor()

    #define list of syms
    syms = ["GOOG","MSFT","AMZN","CRM","AAPL","TSLA","NFLX","META","NVDA","NIKE"]

    global trade_data

    #create row of data
    for i in range(1, batchlength):
        weights = [0.15, 0.15, 0.07, 0.05, 0.08, 0.15, 0.05, 0.15, 0.07, 0.08]
        sym = random.choices(syms, weights=weights, k=1)[0]
        price = 100
        for tup in reversed(trade_data):
            if tup[0] == sym:
                price = (random.gauss(1,0.0001)) * tup[1]
                break
        #return None

        record = (sym, price)
        trade_data.append(record)

    # generate buy/sell side
    side = random.choices(["buy","sell"], k=batchlength+len(syms)-1)


    # generate random times from last 5 seconds and sort
    
   start_time = datetime.datetime.now() - datetime.timedelta(seconds=5)
    end_time = datetime.datetime.now()

    random_times = []
    for i in range(1,batchlength+len(syms)):
        random_time = start_time + datetime.timedelta(seconds=random.randint(0, int((end_time - start_time).total_seconds())))
        random_times.append(random_time)
        sorted_times = sorted(random_times)


    #generate random volumes
    volumes = random.choices(range(10,100),k = batchlength+len(syms)-1)

    #insert data to timescale
    for i, t in enumerate(trade_data):
        cur.execute("INSERT INTO trade (timestamp, sym, price, volume, side) VALUES (%s, %s, %s, %s, %s)", (sorted_times[i],t[0],t[1],volumes[i],side[i]))




    # Commit the changes to the database
    conn.commit()

    # Close the database connection
    cur.close()
    conn.close()

    lastprices = []
    for sym in syms:
        for tup in reversed(trade_data):
            if tup[0] == sym:
                lastprices.append((sym,tup[1]))
                break
    trade_data=lastprices


def repeat_function():
    while True:
        tradebatch(30)
        time.sleep(5)

t = threading.Thread(target=repeat_function)
t.start()
