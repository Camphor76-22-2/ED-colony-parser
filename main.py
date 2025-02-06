import math,json,gzip,getopt,sys,time
from typing import NamedTuple
import orjson


class Coords(NamedTuple):
    x: float
    y: float
    z: float

    def __getitem__(self, item):
        if isinstance(item, int):
            item = self._fields[item]
        return getattr(self, item)

    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, AttributeError, IndexError):
            return default

class Station(NamedTuple):
    name: str

    def __getitem__(self, item):
        if isinstance(item, int):
            item = self._fields[item]
        return getattr(self, item)

    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, AttributeError, IndexError):
            return default

class Body(NamedTuple):
    bodyid: int
    name: str
    stardistance: float
    bodytype: str
    subtype: str
    stations: list
    terraformingstate: str
    isLandable: bool
    rings: list

    
    def __getitem__(self, item):
        if isinstance(item, int):
            item = self._fields[item]
        return getattr(self, item)

    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, AttributeError, IndexError):
            return default


class System(NamedTuple):
    name: str
    coords: Coords
    bodies: list
    population: int

    def __getitem__(self, item):
        if isinstance(item, int):
            item = self._fields[item]
        return getattr(self, item)

    def get(self, item, default=None):
        try:
            return self[item]
        except (KeyError, AttributeError, IndexError):
            return default


populated_systems_within_500ly = {}
sol = System(
            "sol",
            Coords(0,0,0),
            [],
            999,
            )

def process_stations(stations):
    station_list = []
    for stat in stations:
        station_list.append(
            Station(stat['name'])
        )
    return station_list

def process_bodies(bodies):
    bodies_list = []
    for bod in bodies:
        try:
            #if 'subType' in bod.keys() and "giant" in bod['subType']:
            #    print(bod)
            bodies_list.append(
                Body(
                    bod['bodyId'],
                    bod['name'],
                    0 if not 'distanceToArrival' in bod.keys() else bod['distanceToArrival'],
                    bod['type'],
                    "n-a" if not 'subType' in bod.keys() else bod['subType'],
                    process_stations(bod['stations']),
                    'Not terraformable' if not 'terraformingState' in bod.keys() else bod['terraformingState'],
                    False if not "isLandable" in bod.keys() else bool(bod["isLandable"]),
                    [] if not 'rings' in bod.keys() else bod['rings'],
                ))
        except Exception as e:
            print(bod)
            print(f"Failed to process body: {e}")
    return bodies_list


def process_system_line(jline):
    try:
        system=System(
                jline['name'],
                Coords(jline['coords']['x'],jline['coords']['y'],jline['coords']['z']),
                process_bodies(jline['bodies']),
                -1 if not 'population' in jline.keys() else jline['population'],
                )
    except Exception as e:
        print(f"Failed to read system: {e}")
        print(jline)
    if system['population'] > 1000000 and get_system_distance(system,sol)<500 and get_system_distance(system,sol)>140:
        populated_systems_within_500ly[system['name']]=system
        #print(f"Added system to list of possible colony contacts: {system['name']} {get_system_distance(system,sol)}")
    return system

# def get_desired_bodies(system):
#     if system['population']>0 or get_system_distance(system,sol)>500:
#         return []
#     desired_bodies = []
#     count_terraformable = 0
#     count_barycentres = 0
#     for body in system['bodies']:
#         if body['bodytype'] == 'Barycentre':
#             count_barycentres+=1
#             desired_bodies.append(body)
#         elif body['terraformingstate'] == 'Terraformable':
#             count_terraformable+=1
#             desired_bodies.append(body)
#     return desired_bodies if count_terraformable>0 and count_barycentres>0 else []

bodytypes=[]
gas_giant_detail = []

def get_desired_bodies(system,terms):
    if system['population']>0 or get_system_distance(system,sol)>500:
        return []
    desired_bodies = []
    count_terraformable = 0
    count_barycentres = 0
    count_ELW=0
    count_gg=0
    for body in system['bodies']:
        if body['bodytype'] == 'Barycentre':
            count_barycentres+=1
            desired_bodies.append(body)
        elif body['terraformingstate'] == 'Terraformable' and body['isLandable']:
            count_terraformable+=1
            desired_bodies.append(body)
        elif "giant" in body['subtype'] or "giant" in body['bodytype']:
            count_gg+=1
            if terms[0]!=0 and "ring" in terms[0] and len(terms[0].split(":"))>1 and len([x for x in body["rings"] if terms[0].split(":")[1] in x["type"]])>0:
                desired_bodies.append(body)
                #print(body)
        elif "Earth-like world" in body['subtype'] :
            count_ELW+=1
            desired_bodies.append(body)
        if {body['bodytype'],body['subtype']} not in bodytypes:
            bodytypes.append({body['bodytype'],body['subtype']})
    return desired_bodies if count_terraformable+count_barycentres+count_gg>=3 else []

def get_system_distance(s1,s2):
    return math.sqrt(((s1['coords']['x']-s2['coords']['x'])**2)+
    ((s1['coords']['y']-s2['coords']['y'])**2)+
    ((s1['coords']['z']-s2['coords']['z'])**2))

def validate_system_distances(system):
    for spop in populated_systems_within_500ly.keys():
        sp = populated_systems_within_500ly[spop]
        if get_system_distance(system,sp)<10 and get_system_distance(sp,sol)<500:
            return True
    return False    


def readfiledata(filein, terms):
    ljt=0
    rtt=0
    lpt=0
    lrt=0
    ljts=0
    rtts=0
    lpts=0
    lrts=0

    with gzip.open(filein,'r') as file:
        #with io.TextIOWrapper(gfile, encoding='utf-8') as file:
        file.readline()
        records = {}
        n=0
        # zstart=0
        for rawline in file:
            # zread=time.time()
            # rtt=zread-zstart
            n+=1    
            #if n>=10000000:
            #    break
            if n>=1000000: 
                if n%1000000==0:print(f"Scanned lines({int(n/1000000)}m)-({int(((n/1000000)/139)*100)}%)")
            elif n<1000000: 
                if n%1000==0:print(f"Scanned lines({int(n/1000)}k)-({int(((n/1000000)/139)*100)}%)")
            #if n%1000000==0:
            #    for item in gas_giant_detail:
            #        print(item)
            #    print(f"Bodytypes: {bodytypes}")

            # start=time.time()
            line = rawline.decode("utf-8")
            # lineread=time.time()
            # lrt=lineread-start
            try:
                # jstart = time.time()
                jline = orjson.loads(line.strip().strip(","))
                # jload=time.time()
                # ljt=jload-jstart
                try:
                    # pstart = time.time()
                    record = process_system_line(jline)
                    if len(get_desired_bodies(record,terms))>0:
                    #if len([x for x in record['bodies'] if x['bodytype'] not in ["Star","Black Hole","Asteroid Belt"]])>10:
                        records[record['name']]=record
                    # pload=time.time()
                    # lpt=pload-pstart
                    
                except Exception as e:
                    print(f"Trying to validate line: {e}")
                    continue
            except Exception as e:
                print("Failed to read")
                print(e)
            # rtts+=rtt/1000000
            # ljts+=ljt/1000000
            # lpts+=lpt/1000000
            # lrts+=lrt/1000000
            #if n>20000000 : break
            # zstart=time.time()
        #print(f"Gzip time: {math.floor(rtts*1000000)}, Decode time: {math.floor(lrts*1000000)} Json load time: {math.floor(ljts*1000000)}, Processing time: {math.floor(lpts*1000000)}")

        print(len(populated_systems_within_500ly.keys()))
        print(len(records.keys()))
        with open("./records.json",'w') as rfile:
            blob = json.dump(records, rfile)
        with open("./populated.json",'w') as pfile:
            blob = json.dump(populated_systems_within_500ly, pfile)




def main(argv=None):
    if argv is None:
        argv = sys.argv        
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
            if len(argv)>1:
                #print(argv[1])
                tstart=time.time()
                if len(argv)>2:
                    readfiledata(argv[1],[argv[2]])
                else:
                    readfiledata(argv[1],[0])
                tend=time.time()
                diff = math.floor(tend-tstart)
                print(f"Runtime: {math.floor(diff/60)}m{diff % 60}s")
        except Exception as e:
            print(e)



#python3 main.py /mnt/d/Downloads/galaxy.json.gz




if __name__ == '__main__':
    sys.exit(main())