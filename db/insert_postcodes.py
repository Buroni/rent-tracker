import csv
import sqlite3

con = sqlite3.connect("./uk-rent.db")
cur = con.cursor()

with open('./postcodes.csv') as csvfile:
    reader = list(csv.reader(csvfile, delimiter=',', quotechar='|'))
    
    postcodes = []
    i = 1
    for row in reader[1:]:
        postcodes.append(row[0])
    
    cur.execute(f"""
            INSERT INTO
                rightmove_postcode_map (postcode, location_id)
            VALUES
                {",".join(["('" + postcode + "'," + str(i) + ")" for i, postcode in enumerate(postcodes)])}
            ;
    """)

    con.commit()
    
