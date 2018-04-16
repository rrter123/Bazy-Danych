import json
import psycopg2
from sys import stdin

def organizer (secret, newlogin, newpassword):
    if secret == "d8578edf8458ce06fbc5bb76a58c5ca4":
        try:
            cur.execute("INSERT INTO uczestnik (login, haslo, organizator) VALUES (%s, %s, %s)", (newlogin, newpassword, "true"))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error :
            conn.rollback()
            print { "status": "ERROR" }
    else:
        print { "status": "ERROR" }

def event(login, password, eventname, start, end):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s and organizator='t'",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("SELECT wydarzenia(%s, %s, %s);",(eventname, start, end) )
            conn.commit()
            print {'status': cur.fetchone()[0] }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }
                
def user(login, password, newlogin, newpassword):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s and organizator='t'",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("INSERT INTO uczestnik (login, haslo) VALUES (%s, %s);",(newlogin, newpassword) )
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }

def talk (login, password, speakerlogin, talk, title, start, room, initeval, event):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s and organizator='t'",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("SELECT prepare(%s, %s, %s)", (talk, initeval, login))
            cur.execute("UPDATE referat SET autor= %s ;",(speakerlogin,))
            cur.execute("UPDATE referat SET temat= %s ;",(title,))
            cur.execute("UPDATE referat SET poczatek= %s ;",(start,))
            cur.execute("UPDATE referat SET sala= %s ;",(room,))
            if event != "":
                cur.execute("UPDATE referat SET wydarzenieid = %s ;", (event,))
            conn.commit()
            print {'status': "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }
            
def register (login, password, event):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s ",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("INSERT INTO wezmie_udzial VALUES ( %s, %s)", (event, login))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }

def attendance (login, password, talk):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s ",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("INSERT INTO obecnosc VALUES ( %s, %s)", (talk, login))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }
            

def evaluation(login, password, talk, rating):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s ",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("INSERT INTO ocena VALUES ( %s, %s, %s)", (login, talk, rating))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }

def user_plan(login, limit):
    try:
        if limit==0:
            cur.execute("SELECT autor, id, poczatek, temat, sala FROM wezmie_udzial JOIN referat USING (wydarzenieid) WHERE uczestnikid = %s ORDER BY poczatek;", (login,) )
        else:
            cur.execute("SELECT autor, id, poczatek, temat, sala FROM wezmie_udzial JOIN referat USING (wydarzenieid) WHERE uczestnikid = %s ORDER BY poczatek LIMIT %s;", (login,limit))
        temp=[]
        for obj in cur:
            temp2 = {'login': obj[0],
                     'talk':  obj[1],
                     'start_timestamp': obj[2].strftime("%x %X"),
                     'title':  obj[3],
                     'room':  str(obj[4])}
            temp.append(json.dumps(temp2))

        asdf = {"status": "OK", "data" :  temp}
        print asdf
    except psycopg2.Error:
        print { "status": "ERROR" }

def day_plan(time):
    try:
        cur.execute("SELECT id, poczatek, temat, sala FROM referat WHERE poczatek::text LIKE %s ORDER BY sala, poczatek;", (time+"%",))
        temp=[]
        for obj in cur:
            temp2 = {'talk': obj[0],
                     'start_timestamp': obj[1].strftime("%x %X"),
                     'title':  obj[2],
                     'room':  str(obj[3])}
            temp.append(json.dumps(temp2))

        asdf = {"status": "OK", "data" :  temp}
        print asdf
    except psycopg2.Error:
        print { "status": "ERROR" }

def best_talks(start, end, limit, all):
    try:
        if ((all == 1) and (limit == 0) ):
            cur.execute("SELECT id, poczatek, temat, sala FROM referat JOIN ocena on (id=referatid) WHERE poczatek BETWEEN %s and %s  GROUP BY id, poczatek, temat, sala ORDER BY avg(ocena) DESC;", (start, end) )
        elif (all==1):
            cur.execute("SELECT id, poczatek, temat, sala FROM referat JOIN ocena on (id=referatid) WHERE poczatek BETWEEN %s and %s  GROUP BY id, poczatek, temat, sala ORDER BY avg(ocena) DESC LIMIT %s;", (start, end, limit) )
        elif (limit ==0):
            cur.execute("SELECT id, poczatek, temat, sala FROM referat JOIN ocena on (id=referatid) JOIN obecnosc o on (id=o.referatid) WHERE poczatek BETWEEN %s and %s  GROUP BY id, poczatek, temat, sala ORDER BY avg(ocena) DESC;", (start, end) )
        else:
            cur.execute("SELECT id, poczatek, temat, sala FROM referat JOIN ocena on (id=referatid) JOIN obecnosc o on (id=o.referatid) WHERE poczatek BETWEEN %s and %s  GROUP BY id, poczatek, temat, sala ORDER BY avg(ocena) DESC LIMIT %s;", (start, end, limit))


        temp=[]
        for obj in cur:
            temp2 = {'talk': obj[0],
                     'start_timestamp': obj[1].strftime("%x %X"),
                     'title':  obj[2],
                     'room':  str(obj[3])}
            temp.append(json.dumps(temp2))

        asdf = {"status": "OK", "data" :  temp}
        print asdf
    except psycopg2.Error:
        print { "status": "ERROR" }
    
def most_popular_talks(start, end, limit):
    try:
        if limit==0:
            cur.execute("SELECT id, poczatek, temat, sala FROM obecnosc JOIN referat ON (referatid=id) WHERE poczatek BETWEEN %s and %s GROUP BY id, poczatek, temat, sala ORDER BY COUNT(uczestnikid) desc;", (start, end) )
        else:
            cur.execute("SELECT id, poczatek, temat, sala FROM obecnosc JOIN referat ON (referatid=id) WHERE poczatek BETWEEN %s and %s GROUP BY id, poczatek, temat, sala ORDER BY COUNT(uczestnikid) desc LIMIT %s;", (start, end, limit) )
        temp=[]
        for obj in cur:
            temp2 = {'talk': obj[0],
                     'start_timestamp': obj[1].strftime("%x %X"),
                     'title':  obj[2],
                     'room':  str(obj[3])}
            temp.append(json.dumps(temp2))

        asdf = {"status": "OK", "data" :  temp}
        print asdf
    except psycopg2.Error:
        print { "status": "ERROR" }
    

def attended_talks(login, password):
    cur.execute("SELECT * FROM uczestnik WHERE login=%s and haslo = %s ",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("SELECT id, poczatek, temat, sala FROM obecnosc JOIN referat ON (referatid=id) WHERE uczestnikid= %s ",(login,))
            temp=[]
            for obj in cur:
                temp2 = {'talk': obj[0],
                         'start_timestamp': obj[1].strftime("%x %X"),
                         'title':  obj[2],
                         'room':  str(obj[3])}
                temp.append(json.dumps(temp2))
            asdf = {"status": "OK", "data" :  temp}
            print asdf
        except psycopg2.Error:
            print { "status": "ERROR" }

def abandoned_talks(login, password, limit):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s and organizator='t'",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            if limit ==0:
                cur.execute("SELECT id, poczatek, temat, sala FROM obecnosc JOIN referat ON (referatid=id) GROUP BY id, poczatek, temat, sala ORDER BY COUNT(uczestnikid)")
            else:
                cur.execute("SELECT id, poczatek, temat, sala FROM obecnosc JOIN referat ON (referatid=id) GROUP BY id, poczatek, temat, sala ORDER BY COUNT(uczestnikid) LIMIT %s;", (limit,))
            temp=[]
            for obj in cur:
                temp2 = {'talk': obj[0],
                     'start_timestamp': obj[1].strftime("%x %X"),
                     'title':  obj[2],
                     'room':  str(obj[3])}
                temp.append(json.dumps(temp2))
            asdf = {"status": "OK", "data" :  temp}
            print asdf
        except psycopg2.Error:
            print { "status": "ERROR" }

def friends(log1, password, log2):
    cur.execute("SELECT * FROM uczestnik WHERE login=%s and haslo = %s ",(log1, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("INSERT INTO znajomosc VALUES (%s, %s);", (log1, log2))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }
            
def proposal(login, password, talk, title, start):
    cur.execute("SELECT * FROM uczestnik WHERE login=%s and haslo = %s ",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("INSERT INTO referat (id, autor, temat, poczatek) VALUES (%s, %s, %s, %s);", (talk, login, title,start))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }
def reject(login, password, talk):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s and organizator='t'",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("DELETE FROM referat where id = %s;", (talk,))
            conn.commit()
            print { "status": "OK" }
        except psycopg2.Error:
            conn.rollback()
            print { "status": "ERROR" }

def proposals(login, password):
    cur.execute("SELECT* FROM uczestnik WHERE login=%s and haslo = %s and organizator='t'",(login, password ))
    if (cur.fetchone() == []):
        print { "status": "ERROR" }
    else:
        try:
            cur.execute("SELECT id, poczatek, temat, sala FROM referat WHERE sala IS NULL or wydarzenieid IS NULL;")
            temp=[]
            for obj in cur:
                temp2 = {'talk': obj[0],
                     'start_timestamp': obj[1].strftime("%x %X"),
                     'title':  obj[2],
                     'room':  str(obj[3])}
                temp.append(json.dumps(temp2))
            asdf = {"status": "OK", "data" :  temp}
            print asdf
        except psycopg2.Error:
            print { "status": "ERROR" }

    


#MAIN
danebd = json.loads(stdin.readline())
try:
    conn = psycopg2.connect(dbname=danebd["open"]["baza"], user=danebd["open"]["login"], password=danebd["open"]["password"])
    print { "status": "OK" }
except psycopg2.Error :
    print { "status": "ERROR" }
    
#{"open": {"baza": "Alicja", "login": "Alicja", "password": "Alicja"}}
#{"friends": {"login1": "usr0", "password": "qwerty0", "login2": "usr1"}}
#{"user_plan": {"login": "usr1", "limit": 100}}

cur = conn.cursor()
try:
    cur.execute("SELECT * FROM wydarzenie;")
    cur.fetchall()
except psycopg2.Error :
    conn.rollback()
    cur.execute(open("a.sql", "r").read())
    conn.commit()


temp = stdin.readline()
while 1:
    if temp=="\n":
        break
    line = json.loads(temp)
    case = (line.keys())[0]
    line = line[case]
    if case == "organizer":
        organizer(line["secret"],line["newlogin"],line["newpassword"])
    elif case == "event":
        event(line["login"],line["password"], line["eventname"], line["start_timestamp"], line["end_timestamp"])
    elif case == "user":
        user(line["login"],line["password"],line["newlogin"],line["newpassword"])
    elif case == "talk":
        talk(line["login"], line["password"], line["speakerlogin"], line["talk"], line["title"], line["start_timestamp"], line["room"], line["initial_evaluation"], line["eventname"])
    elif case == "register_user_for_event":
        register(line["login"], line["password"], line["eventname"])
    elif case=="attendance":
        attendance(line["login"], line["password"], line["talk"])
    elif case=="evaluation":
        evaluation(line["login"], line["password"], line["talk"], line["rating"])
    elif case=="user_plan":
        user_plan(line["login"], line["limit"])
    elif case=="day_plan":
        day_plan(line["timestamp"])
    elif case == "best_talks":
        best_talks(line["start_timestamp"], line["end_timestamp"], line["limit"], line["all"])
    elif case == "attended_talks":
        attended_talks(line["login"],line["password"])
    elif case == "most_popular_talks":
        most_popular_talks(line["start_timestamp"], line["end_timestamp"], line["limit"])
    elif case == "abandoned_talks":
        abandoned_talks(line["login"], line["password"], line["limit"])
    elif case=="friends":
        friends(line["login1"], line["password"], line["login2"])
    elif case == "proposal":
        proposal(line["login"], line["password"], line["talk"], line["title"], line["start_timestamp"])
    elif case=="reject":
        reject(line["login"], line["password"], line["talk"])
    elif case == "proposals":
        proposals(line["login"], line["password"])
    else:
        print { "status": "NOT IMPLEMENTED" }

    temp = stdin.readline()

cur.close()
conn.close()
