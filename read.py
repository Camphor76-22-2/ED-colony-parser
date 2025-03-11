import math,json,gzip,getopt,sys
from typing import NamedTuple

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


def get_system_distance(s1,s2):
    try:
        return math.sqrt(((s1[1][0]-s2[1][0])**2)+
        ((s1[1][1]-s2[1][1])**2)+
        ((s1[1][2]-s2[1][2])**2))
    except Exception as e:
        print(f"Failed to check distances {e}")


def validate_system_distances(system):
    global populated_systems_within_500ly
    try:
        for spop in populated_systems_within_500ly.keys():
            sp = populated_systems_within_500ly[spop]
            poggg=[x for x in system[2] if len(x[8])>0 and len([y for y in x[8] if y["type"] in ["Metal Rich","Icy"]])>1 and x[2]<2000]
            if get_system_distance(system,sp)<16 and get_system_distance(sp,sol)<500 and system[1][0]<0 and system[1][2]>0:
                if len(poggg)>0:        
                    #print(poggg)     
                    return [system,sp]
    except Exception as e:
        print(f"Failed to validate system: {e}")
    return []   


def main(argv=None):
    if argv is None:
        argv = sys.argv        
        try:
            opts, args = getopt.getopt(argv[1:], "h", ["help"])
            global populated_systems_within_500ly
            recordfile="./records.json"
            if len(args)>1:
                recordfile=args[1]
            with open(recordfile,'r') as rfile:
                with open("./populated.json",'r') as pfile:
                    populated_systems_within_500ly = json.load(pfile)
                    records = json.load(rfile)
                    for record in records.keys():
                        filtered_results = validate_system_distances(records[record])
                        if len(filtered_results)>0:
                            print(f"\n{records[record][0]}, {filtered_results[1][0]}, bodies:{len(records[record][2])}")
        except Exception as e:
            print(e)


if __name__ == '__main__':
    sys.exit(main())