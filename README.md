# ED-colony-parser
Little python tool for reading out data from the spansh 80gb+ galaxy dump, looks for unpopulated systems with at least 3 of: barycenters, terraformable worlds, and optionally gas giants within 10 ly of a high pop system

Expect 3gb+ ram use if using the rings option, as that stores about twice as much data in ram

Runs in about 2-3 hours in a VM, faster on my computer baremetal so ymmv, very disk speed bound so desireable to have this on an ssd
# Install
```
#Have python3 installed
python3 -m pip install orjson

#clone in a bash derivitive terminal, git bash works well on windows, or your preferred flavor for linux/unix

cd <where you want to put the galaxy dump>

git clone https://github.com/MadParkerD/ED-colony-parser.git
_or_
git clone git@github.com:MadParkerD/ED-colony-parser.git
_or_
download the zip and unzip in your desired folder

# Download the spansh full galaxy dump to the same drive this program is cloned to
```



# Running: Requires the spansh 80gb+ zipped dump stored locally (do not unzip)
```
python3 main.py <dump_location>
#or
#also consider gas giants with rings of <type> where valid types are "Metal, Icy, Rocky" 
python3 main.py <dump_location> ring:Icy

# Run the distance calculations to make the data more meaningful and exportable
python3 read.py
# optional argument specifies a different file name
# if e.g. you want multiple outputs rename the records.json file before re-running main.py
python3 read.py ./icy_ring_records.json
