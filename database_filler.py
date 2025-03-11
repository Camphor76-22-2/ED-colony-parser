import mariadb
import math,json,gzip,getopt,sys,time
from typing import NamedTuple
import orjson

conn=None
def get_conn():
    try:
        conn = mariadb.connect(
            user="galaxydumper",
            password="galaxydumper",
            host="127.0.0.1",
            port=3306,
            database="galaxy"
        )
        return conn
    except Exception as e:
        print(f"Failed to open connection to database: {e}")


def ensure_setup(cur):
    cur.execute(
"""CREATE TABLE IF NOT EXISTS systems(
id64 BIGINT,
x int,
y int,
z int,
name varchar(255) not null,
allegiance varchar(255),
government varchar(255),
primaryEconomy varchar(255),
secondaryEconomy varchar(255),
security varchar(255),
population BIGINT,
bodyCount int,
date DATETIME,
PRIMARY key(id64)
)"""
)
    cur.execute(
"""CREATE TABLE IF NOT EXISTS bodies(
system_id64 BIGINT,
body_id64 BIGINT,
name varchar(255) not null,
type varchar(255),
subtype varchar(255),
distance_arv FLOAT,
mainStar varchar(255),
age int,
spectralClass varchar(255),
luminosity varchar(255),
absoluteMagnitude FLOAT,
solarMasses FLOAT,
solarRadius FLOAT,
surfaceTemperature FLOAT,
rotationalPeriod FLOAT,
axialTilt FLOAT,
parents varchar(255),
orbitalPeriod FLOAT,
semiMajorAxis FLOAT,
orbitalEccentricity FLOAT,
orbitalInclination FLOAT,
argOfPeriapsis FLOAT,
meanAnomaly FLOAT,
ascendingNode FLOAT,
timestamps varchar(255),
updateTime DATETIME,
PRIMARY key(body_id64),
CONSTRAINT `fk_system_id64`
    FOREIGN KEY (system_id64) REFERENCES systems (id64)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
)"""
    )
    cur.execute(
"""CREATE TABLE IF NOT EXISTS stations(
id BIGINT,
system_id64 BIGINT,
name varchar(255) not null,
updateTime DATETIME,
controllingFaction varchar(255),
controllingFactionState varchar(255),
distanceToArrival FLOAT,
primaryEconomy varchar(255),
economies varchar(255),
government varchar(255),
services TEXT,
type varchar(255),
landingPads varchar(255),
market TEXT,
PRIMARY key(id),
CONSTRAINT `fk_system_id64_stations`
    FOREIGN KEY (system_id64) REFERENCES systems (id64)
    ON DELETE CASCADE
    ON UPDATE RESTRICT
)"""
)
    
def get_system_distance(s1,s2):
    try:
        return math.sqrt(((s1[0]-s2[0])**2)+
        ((s1[1]-s2[1])**2)+
        ((s1[2]-s2[2])**2))
    except Exception as e:
        print(f"Failed to check distances {e}")


def update_stations(cur, stations,id64):
    udt=None
    try: udt = stations['updateTime'].split("+")[0]
    except: pass
    try:
        cur.execute(
"""INSERT INTO stations(id,system_id64,name,updateTime,controllingFaction,controllingFactionState,distanceToArrival,primaryEconomy,economies,government,services,type,landingPads,market)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE controllingFaction=?,controllingFactionState=?,primaryEconomy=?,economies=?,government=?,services=?,landingPads=?,market=?;""",
(stations["id"],
id64,
stations['name'],
udt,
None if not 'controllingFaction' in stations.keys() else stations['controllingFaction'],
None if not 'controllingFactionState' in stations.keys() else stations['controllingFactionState'],
0 if not 'distanceToArrival' in stations.keys() else stations['distanceToArrival'],
None if not 'primaryEconomy' in stations.keys() else stations['primaryEconomy'],
None if not 'economies' in stations.keys() else stations['economies'],
None if not 'government' in stations.keys() else stations['government'],
None if not 'services' in stations.keys() else stations['services'],
None if not 'type' in stations.keys() else stations['type'],
None if not 'landingPads' in stations.keys() else stations['landingPads'],
None if not 'market' in stations.keys() else stations['market'],
None if not 'controllingFaction' in stations.keys() else stations['controllingFaction'],
None if not 'controllingFactionState' in stations.keys() else stations['controllingFactionState'],
None if not 'primaryEconomy' in stations.keys() else stations['primaryEconomy'],
None if not 'economies' in stations.keys() else stations['economies'],
None if not 'government' in stations.keys() else stations['government'],
None if not 'services' in stations.keys() else stations['services'],
None if not 'landingPads' in stations.keys() else stations['landingPads'],
None if not 'market' in stations.keys() else stations['market'],
)
)
    except mariadb.Error as e: 
        print(f"Error: {e}")



def update_bodies(cur, bodies, id64):
    udt = None
    if 'updateTime' in bodies.keys():
        udt = bodies['updateTime'].split("+")[0]
    try:
        cur.execute(
"""INSERT INTO bodies(system_id64,body_id64,name,type,subtype,distance_arv,mainStar,age,spectralClass,luminosity,absoluteMagnitude,solarMasses,solarRadius,surfaceTemperature,rotationalPeriod,axialTilt,parents,orbitalPeriod,semiMajorAxis,orbitalEccentricity,orbitalInclination,argOfPeriapsis,meanAnomaly,ascendingNode,timestamps,updateTime)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE updateTime=?;""",
(id64,
bodies["id64"],
bodies["name"],
None if not 'type' in bodies.keys() else bodies['type'],
None if not 'subtype' in bodies.keys() else bodies['subtype'],
None if not 'distance_arv' in bodies.keys() else bodies['distance_arv'],
None if not 'mainStar' in bodies.keys() else bodies['mainStar'],
None if not 'age' in bodies.keys() else bodies['age'],
None if not 'spectralClass' in bodies.keys() else bodies['spectralClass'],
None if not 'luminosity' in bodies.keys() else bodies['luminosity'],
None if not 'absoluteMagnitude' in bodies.keys() else bodies['absoluteMagnitude'],
None if not 'solarMasses' in bodies.keys() else bodies['solarMasses'],
None if not 'solarRadius' in bodies.keys() else bodies['solarRadius'],
None if not 'surfaceTemperature' in bodies.keys() else bodies['surfaceTemperature'],
None if not 'rotationalPeriod' in bodies.keys() else bodies['rotationalPeriod'],
None if not 'axialTilt' in bodies.keys() else bodies['axialTilt'],
None if not 'parents' in bodies.keys() else bodies['parents'],
None if not 'orbitalPeriod' in bodies.keys() else bodies['orbitalPeriod'],
None if not 'semiMajorAxis' in bodies.keys() else bodies['semiMajorAxis'],
None if not 'orbitalEccentricity' in bodies.keys() else bodies['orbitalEccentricity'],
None if not 'orbitalInclination' in bodies.keys() else bodies['orbitalInclination'],
None if not 'argOfPeriapsis' in bodies.keys() else bodies['argOfPeriapsis'],
None if not 'meanAnomaly' in bodies.keys() else bodies['meanAnomaly'],
None if not 'ascendingNode' in bodies.keys() else bodies['ascendingNode'],
None if not 'timestamps' in bodies.keys() else bodies['timestamps'],
udt,
udt
)
)
    except mariadb.Error as e: 
        print(f"Error: {e}")


def update_system(cur,jline):
    date = jline['date'].split("+")[0]
    #print(jline)
    try:
        cur.execute(
"""INSERT INTO systems(id64,x,y,z,name,allegiance,government,primaryEconomy,secondaryEconomy,security,population,bodyCount,date)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE allegiance=?,government=?,primaryEconomy=?,secondaryEconomy=?,security=?,population=?,date=?;""",
(jline["id64"],
jline['coords']['x'],
jline['coords']['y'],
jline['coords']['z'],
jline['name'],
None if not 'allegiance' in jline.keys() else jline['allegiance'],
None if not 'government' in jline.keys() else jline['government'],
None if not 'primaryEconomy' in jline.keys() else jline['primaryEconomy'],
None if not 'secondaryEconomy' in jline.keys() else jline['secondaryEconomy'],
None if not 'security' in jline.keys() else jline['security'],
0 if not 'population' in jline.keys() else jline['population'],
None if not 'bodyCount' in jline.keys() else jline['bodyCount'],
date,
None if not 'allegiance' in jline.keys() else jline['allegiance'],
None if not 'government' in jline.keys() else jline['government'],
None if not 'primaryEconomy' in jline.keys() else jline['primaryEconomy'],
None if not 'secondaryEconomy' in jline.keys() else jline['secondaryEconomy'],
None if not 'security' in jline.keys() else jline['security'],
0 if not 'population' in jline.keys() else jline['population'],
date,
)
)
    except mariadb.Error as e: 
        print(f"Error: {e}")
    conn.commit()
    if "stations" in jline.keys():
        for station in jline["stations"]:
            update_stations(cur,station,jline["id64"])
    conn.commit()
    if "bodies" in jline.keys():
        for body in jline["bodies"]:
            update_bodies(cur,body,jline["id64"])
    conn.commit()
    




def load_messages(cur,filein):
    with gzip.open(filein,'r') as file:
        file.readline()
        n=0
        for line in file:
            rline = line.decode("utf-8")
            jline = orjson.loads(rline.strip().strip(","))
            if get_system_distance([0,0,0],[jline['coords']['x'],jline['coords']['y'],jline['coords']['z']])<10000:
                update_system(cur,jline)


def main():
    argv = sys.argv        
    if len(argv)>1:
        global conn
        conn = get_conn()
        cur = conn.cursor()
        ensure_setup(cur)
        load_messages(cur,argv[1])
        conn.commit()
        conn.close()
    else:
        print("File path not present in command")



if __name__ == '__main__':
    sys.exit(main())